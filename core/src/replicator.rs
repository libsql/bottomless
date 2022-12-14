use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    write_buffer: HashMap<u32, (i32, BytesMut)>,
    runtime: tokio::runtime::Runtime,

    generation: uuid::Uuid,
    next_frame: u32,
    pub(crate) bucket: String,
    pub(crate) db_path: String,
    pub(crate) db_name: String,
}

#[derive(Debug)]
pub struct FetchedResults {
    pub pages: Vec<(i32, Bytes)>,
    pub next_marker: Option<String>,
}

#[derive(Debug)]
pub enum RestoreAction {
    None,
    SnapshotMainDbFile,
    ReuseGeneration(uuid::Uuid),
}

impl Replicator {
    // FIXME: this should be derived from the database config
    // and preserved somewhere in order to only restore from
    // matching page size.
    pub const PAGE_SIZE: usize = 4096;

    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let write_buffer = HashMap::new();
        let client = runtime.block_on(async {
            let mut loader = aws_config::from_env();
            if let Ok(endpoint) = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT") {
                loader = loader.endpoint_resolver(Endpoint::immutable(endpoint.parse()?));
            }
            Ok::<Client, anyhow::Error>(Client::new(&loader.load().await))
        })?;
        let bucket =
            std::env::var("LIBSQL_BOTTOMLESS_BUCKET").unwrap_or_else(|_| "bottomless".to_string());
        let generation = Self::generate_generation();
        tracing::debug!("Generation {}", generation);
        Ok(Self {
            client,
            write_buffer,
            runtime,
            bucket,
            generation,
            /* NOTICE: Next frame is 1 only if we always checkpoint on boot,
             ** and start from a fresh snapshot of a database file
             ** with an empty WAL. If this is not enforced, next frame
             ** should be deduced from the replicated contents - it's the first
             ** unused frame number from the latest generation.
             */
            next_frame: 1,
            db_path: String::new(),
            db_name: String::new(),
        })
    }

    fn get_object(&self, key: String) -> aws_sdk_s3::client::fluent_builders::GetObject {
        self.client.get_object().bucket(&self.bucket).key(key)
    }

    fn list_objects(&self) -> aws_sdk_s3::client::fluent_builders::ListObjects {
        self.client.list_objects().bucket(&self.bucket)
    }

    fn generate_generation() -> uuid::Uuid {
        // This timestamp goes back in time - that allows us to list newest generations
        // first in the S3 bucket.
        let (seconds, nanos) = uuid::timestamp::Timestamp::now(uuid::NoContext).to_unix();
        let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
        let synthetic_ts = uuid::Timestamp::from_unix(uuid::NoContext, seconds, nanos);
        uuid::Uuid::new_v7(synthetic_ts)
    }

    pub fn new_generation(&mut self) {
        self.generation = Self::generate_generation();
        tracing::debug!("New generation started: {}", self.generation);
    }

    pub fn set_generation(&mut self, generation: uuid::Uuid) {
        self.generation = generation;
        tracing::info!("Generation set to {}", self.generation);
    }

    pub fn register_db(&mut self, db_path: impl Into<String>) {
        assert!(self.db_name.is_empty());
        let db_path = db_path.into();
        let name = match db_path.rfind('/') {
            Some(index) => db_path[index + 1..].to_string(),
            None => db_path.to_string(),
        };
        self.db_path = db_path;
        self.db_name = name;
        tracing::debug!(
            "Registered name: {} (full path: {})",
            self.db_name,
            self.db_path
        );
    }

    pub fn next_frame(&mut self) -> u32 {
        self.next_frame += 1;
        self.next_frame - 1
    }

    pub fn write(&mut self, pgno: i32, data: &[u8]) {
        let frame = self.next_frame();
        tracing::info!("Writing page {}:{} at frame {}", pgno, data.len(), frame);
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(data);
        self.write_buffer.insert(frame, (pgno, bytes));
    }

    // Sends the pages participating in a commit to S3
    // FIXME: Newest consistent frame number needs to be stored right after committing
    // in order to be able to recover from a partial commit later
    pub fn commit(&mut self) -> Result<()> {
        tracing::info!("Write buffer size: {}", self.write_buffer.len());
        self.runtime.block_on(async {
            let tasks = self.write_buffer.iter().map(|(frame, (pgno, bytes))| {
                let data: &[u8] = bytes;
                if data.len() != Self::PAGE_SIZE {
                    tracing::warn!("Unexpected truncated page of size {}", data.len())
                }
                // NOTICE: Current format is <generation>/<db-name>-<frame-number>-<page-number>
                let key = format!(
                    "{}-{}/{:012}-{:012}",
                    self.db_name, self.generation, frame, pgno
                );
                tracing::info!("Committing {}", key);
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(ByteStream::from(data.to_owned()))
                    .send()
            });
            futures::future::try_join_all(tasks).await?;
            self.write_buffer.clear();
            // Last consistent frame is persisted in S3 in order to be able to recover
            // from failured that happen in the middle of a commit, when only some
            // of the pages that belong to a transaction are replicated.
            let last_consistent_frame_key =
                format!("{}-{}/.consistent", self.db_name, self.generation);
            tracing::info!("Last consistent frame: {}", self.next_frame - 1);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(last_consistent_frame_key)
                .body(ByteStream::from(Bytes::copy_from_slice(
                    &(self.next_frame - 1).to_be_bytes(),
                )))
                .send()
                .await?;
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }

    async fn read_change_counter(reader: &mut tokio::fs::File) -> Result<[u8; 4]> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24)).await?;
        reader.read_exact(&mut counter).await?;
        Ok(counter)
    }

    async fn open_buffered(path: &str) -> Result<tokio::io::BufReader<tokio::fs::File>> {
        Ok(tokio::io::BufReader::new(
            tokio::fs::File::open(path).await?,
        ))
    }

    // Returns the compressed database file path and its change counter, extracted
    // from the header of page1 at offset 24..27 (as per SQLite documentation).
    pub async fn compress_main_db_file(&self) -> Result<(&'static str, [u8; 4])> {
        use tokio::io::AsyncWriteExt;
        let compressed_db = "db.gz";
        let mut reader = tokio::fs::File::open(&self.db_path).await?;
        let mut writer = async_compression::tokio::write::GzipEncoder::new(
            tokio::fs::File::create(compressed_db).await?,
        );
        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.shutdown().await?;
        let change_counter = Self::read_change_counter(&mut reader).await?;
        Ok((compressed_db, change_counter))
    }

    // Sends the main database file to S3
    pub fn snapshot_main_db_file(&mut self) -> Result<()> {
        if !std::path::Path::new(&self.db_path).exists() {
            tracing::info!("Not snapshotting, the main db file does not exist");
            return Ok(());
        }
        tracing::debug!("Snapshotting {}", self.db_path);

        // The main file is compressed, because snapshotting is rare, and libSQL pages
        // are often sparse, so they compress well.
        // TODO: find a way to compress ByteStream on the fly instead of creating
        // an intermediary file.

        self.runtime.block_on(async {
            let (compressed_db_path, change_counter) = self.compress_main_db_file().await?;
            let key = format!("{}-{}/db.gz", self.db_name, self.generation);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from_path(compressed_db_path).await?)
                .send()
                .await?;
            let change_counter_key = format!("{}-{}/.changecounter", self.db_name, self.generation);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(change_counter_key)
                .body(ByteStream::from(Bytes::copy_from_slice(&change_counter)))
                .send()
                .await?;
            Ok::<(), anyhow::Error>(())
        })?;
        tracing::debug!("Main db snapshot complete");
        Ok(())
    }

    async fn find_newest_generation_async(&self) -> Option<uuid::Uuid> {
        let response = self.list_objects().max_keys(1).send().await.ok()?;
        let objs = response.contents()?;
        let key = objs.first()?.key()?;
        let key = match key.find('/') {
            Some(index) => &key[0..index],
            None => key,
        };
        tracing::info!("Generation candidate: {}", key);
        uuid::Uuid::parse_str(key).ok()
    }

    //FIXME: assumes that this bucket stores *only* generations,
    // it should be more robust
    fn find_newest_generation(&self) -> Option<uuid::Uuid> {
        self.runtime
            .block_on(async { self.find_newest_generation_async().await })
    }

    async fn get_remote_change_counter(&self, generation: &uuid::Uuid) -> Result<[u8; 4]> {
        use bytes::Buf;
        let mut remote_change_counter = [0u8; 4];
        if let Ok(response) = self
            .get_object(format!("{}-{}/.changecounter", self.db_name, generation))
            .send()
            .await
        {
            response
                .body
                .collect()
                .await?
                .copy_to_slice(&mut remote_change_counter)
        }
        Ok(remote_change_counter)
    }

    async fn get_last_consistent_frame(&self, generation: &uuid::Uuid) -> Result<u32> {
        use bytes::Buf;
        Ok(
            match self
                .get_object(format!("{}-{}/.consistent", self.db_name, generation))
                .send()
                .await
                .ok()
            {
                Some(response) => response.body.collect().await?.get_u32(),
                None => 0,
            },
        )
    }

    async fn get_wal_page_count(&self) -> u32 {
        match tokio::fs::File::open(&format!("{}-wal", &self.db_path)).await {
            Ok(file) => {
                let metadata = match file.metadata().await {
                    Ok(metadata) => metadata,
                    Err(_) => return 0,
                };
                // Each WAL file consists of a 32-byte WAL header and N entries of size (page size + 24)
                (metadata.len() / (Self::PAGE_SIZE + 24) as u64) as u32
            }
            Err(_) => 0,
        }
    }

    fn parse_frame_and_page_numbers(key: &str) -> Option<(u32, i32)> {
        // Format: <generation>/<db-name>-<frame-number>-<page-number>
        let page_delim = key.rfind('-')?;
        let frame_delim = key[0..page_delim].rfind('-')?;
        let frameno = key[frame_delim + 1..page_delim].parse::<u32>().ok()?;
        let pgno = key[page_delim + 1..].parse::<i32>().ok()?;
        Some((frameno, pgno))
    }

    pub async fn restore_from(&self, generation: uuid::Uuid) -> Result<RestoreAction> {
        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_change_counter = match tokio::fs::File::open(&self.db_path).await {
            Ok(mut db) => Self::read_change_counter(&mut db).await?,
            Err(_) => [0u8; 4],
        };
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let remote_change_counter = self.get_remote_change_counter(&generation).await?;
        tracing::info!(
            "Counters: local={:?}, remote={:?}",
            local_change_counter,
            remote_change_counter
        );

        let last_consistent_frame = self.get_last_consistent_frame(&generation).await?;
        tracing::info!("Last consistent remote frame: {}", last_consistent_frame);

        if local_change_counter == remote_change_counter {
            let wal_pages = self.get_wal_page_count().await;
            tracing::warn!(
                "Consistent: {}; wal pages: {}",
                last_consistent_frame,
                wal_pages
            );
            if wal_pages == last_consistent_frame {
                tracing::warn!(
                    "Newest remote generation is up-to-date, reusing it in this session"
                );
                return Ok(RestoreAction::ReuseGeneration(generation));
            }
        } else if local_change_counter > remote_change_counter {
            tracing::warn!("Local change counter is larger than its remote counterpart - a new snapshot needs to be replicated");
            // FIXME: checkpoint needs to be performed here - apply all WAL pages
            // to the main file before snapshotting.
            // An alternative is to just replicate all WAL pages, but it sounds like a waste of throughput.
            return Ok(RestoreAction::SnapshotMainDbFile);
        }

        let db_file = self
            .get_object(format!("{}-{}/db.gz", self.db_name, generation))
            .send()
            .await?;
        // TODO: decompress on the fly, without a separate file
        let compressed_db_path = "db.restored.gz";
        let mut body_reader = db_file.body.into_async_read();
        let mut compressed_writer = tokio::fs::File::create(compressed_db_path).await?;
        tokio::io::copy(&mut body_reader, &mut compressed_writer).await?;
        compressed_writer.flush().await?;
        let mut decompressed_reader = async_compression::tokio::bufread::GzipDecoder::new(
            Self::open_buffered(compressed_db_path).await?,
        );
        let mut main_db_writer = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&self.db_path)
            .await?;
        tokio::io::copy(&mut decompressed_reader, &mut main_db_writer).await?;
        main_db_writer.flush().await?;
        tracing::info!("Restored main db file");

        let mut next_marker = None;
        let prefix = format!("{}-{}/", self.db_name, generation);
        // FIXME: consider what to do with WAL present - if the change counters
        // match and checksums do too, some of it could be applied locally first
        tracing::warn!("Overwriting any existing WAL file: {}-wal", &self.db_path);
        tokio::fs::remove_file(&format!("{}-wal", &self.db_path))
            .await
            .ok();
        tokio::fs::remove_file(&format!("{}-shm", &self.db_path))
            .await
            .ok();
        loop {
            let mut list_request = self.list_objects().prefix(&prefix);
            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker);
            }
            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(objs) => objs,
                None => return Ok(RestoreAction::SnapshotMainDbFile),
            };
            //TODO: consider higher concurrency
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                tracing::debug!("Loading {}", key);
                let frame = self.get_object(key.into()).send().await?;

                let (frameno, pgno) = match Self::parse_frame_and_page_numbers(key) {
                    Some(result) => result,
                    None => {
                        tracing::debug!("Failed to parse frame/page from key {}", key);
                        continue;
                    }
                };
                if frameno > last_consistent_frame {
                    tracing::warn!("Remote log contains frame {} larger than last consistent frame ({}), stopping the restoration",
                                frameno, last_consistent_frame);
                    break;
                }
                let mut data = frame.body.into_async_read();
                let offset = pgno as u64 * Self::PAGE_SIZE as u64;
                main_db_writer
                    .seek(tokio::io::SeekFrom::Start(offset))
                    .await?;
                // FIXME: we only need to overwrite with the newest page,
                // no need to replay the whole WAL
                tokio::io::copy(&mut data, &mut main_db_writer).await?;
                main_db_writer.flush().await?;
                tracing::info!("Written frame {} as main db page {}", frameno, pgno);
            }
            next_marker = response
                .is_truncated()
                .then(|| objs.last().map(|elem| elem.key().unwrap().to_string()))
                .flatten();
            if next_marker.is_none() {
                break;
            }
        }

        Ok::<RestoreAction, anyhow::Error>(RestoreAction::SnapshotMainDbFile)
    }

    pub fn restore(&self) -> Result<RestoreAction> {
        let newest_generation = match self.find_newest_generation() {
            Some(gen) => gen,
            None => {
                tracing::info!("No generation found, nothing to restore");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
        };

        tracing::info!("Restoring from generation {}", newest_generation);
        self.runtime
            .block_on(async { self.restore_from(newest_generation).await })
    }
}

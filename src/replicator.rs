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
        let endpoint = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9000".to_string());
        let client = runtime.block_on(async {
            Ok::<Client, anyhow::Error>(Client::new(
                &aws_config::from_env()
                    .endpoint_resolver(Endpoint::immutable(endpoint.parse()?))
                    .load()
                    .await,
            ))
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

    fn generate_generation() -> uuid::Uuid {
        // This timestamp goes back in time - that allows us to list newest generations
        // first in the S3 bucket.
        let (seconds, nanos) = uuid::timestamp::Timestamp::now(uuid::NoContext).to_unix();
        let (seconds, nanos) = (u64::MAX / 1000 - seconds, 999999999 - nanos);
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
                    "{}/{}-{:012}-{:012}",
                    self.generation, self.db_name, frame, pgno
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
                format!("{}/{}-consistent", self.generation, self.db_name);
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

    fn read_change_counter(reader: &mut std::fs::File) -> Result<[u8; 4]> {
        use std::io::{Read, Seek};
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24))?;
        reader.read_exact(&mut counter)?;
        Ok(counter)
    }

    // Returns the compressed database file path and its change counter, extracted
    // from the header of page1 at offset 24..27 (as per SQLite documentation).
    pub fn compress_main_db_file(&self) -> Result<(String, [u8; 4])> {
        let compressed_db = format!("{}.lz4", &self.db_path);
        let mut reader = std::fs::File::open(&self.db_path)?;
        let mut writer = lz4_flex::frame::FrameEncoder::new(std::fs::File::create(&compressed_db)?);
        std::io::copy(&mut reader, &mut writer)?;
        writer.finish()?;
        let change_counter = Self::read_change_counter(&mut reader)?;
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
        let (compressed_db_path, change_counter) = self.compress_main_db_file()?;

        self.runtime.block_on(async {
            let key = format!("{}/{}.lz4", self.generation, self.db_name);
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from_path(&compressed_db_path).await?)
                .send()
                .await?;
            let change_counter_key = format!("{}/{}.changecounter", self.generation, self.db_name);
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

    //FIXME: assumes that this bucket stores *only* generations,
    // it should be more robust
    fn find_newest_generation(&self) -> Option<uuid::Uuid> {
        self.runtime.block_on(async {
            let response = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .max_keys(1)
                .send()
                .await
                .ok()?;
            let objs = response.contents()?;
            let key = objs.first()?.key()?;
            let key = match key.find('/') {
                Some(index) => &key[0..index],
                None => key,
            };
            tracing::info!("Generation candidate: {}", key);
            uuid::Uuid::parse_str(key).ok()
        })
    }

    async fn get_remote_change_counter(&self, generation: &uuid::Uuid) -> Result<[u8; 4]> {
        use bytes::Buf;
        let mut remote_change_counter = [0u8; 4];
        if let Ok(response) = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&format!("{}/{}.changecounter", generation, self.db_name))
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
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&format!("{}/{}-consistent", generation, self.db_name))
                .send()
                .await
                .ok()
            {
                Some(response) => response.body.collect().await?.get_u32(),
                None => 0,
            },
        )
    }

    async fn get_wal_page_count(&self) -> Result<u32> {
        Ok(
            match tokio::fs::File::open(&format!("{}-wal", &self.db_path))
                .await
                .ok()
            {
                // Each WAL file consists of a 32-byte WAL header and N entries of size (page size + 24)
                Some(file) => (file.metadata().await?.len() / (Self::PAGE_SIZE + 24) as u64) as u32,
                None => 0,
            },
        )
    }

    fn parse_frame_and_page_numbers(key: &str) -> Option<(u32, i32)> {
        // Format: <generation>/<db-name>-<frame-number>-<page-number>
        let page_delim = key.rfind('-')?;
        let frame_delim = key[0..page_delim].rfind('-')?;
        let frameno = key[frame_delim + 1..page_delim].parse::<u32>().ok()?;
        let pgno = key[page_delim + 1..].parse::<i32>().ok()?;
        Some((frameno, pgno))
    }

    // FIXME: commit() needs to save the last consistent frame number,
    // and it needs to be taken into account here as well - only whole
    // valid transactions should be restored to WAL
    pub fn restore(&self) -> Result<RestoreAction> {
        let newest_generation = match self.find_newest_generation() {
            Some(gen) => gen,
            None => {
                tracing::info!("No generation found, nothing to restore");
                return Ok(RestoreAction::None);
            }
        };

        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_change_counter =
            Self::read_change_counter(&mut std::fs::File::open(&self.db_path)?)?;

        tracing::info!("Restoring from generation {}", newest_generation);
        self.runtime.block_on(async {
            use std::io::Write;
            use tokio::io::{AsyncSeekExt, AsyncWriteExt};

            let remote_change_counter = self.get_remote_change_counter(&newest_generation).await?;

            tracing::warn!(
                "Change counters: local={:?}, remote={:?}",
                local_change_counter,
                remote_change_counter
            );

            let last_consistent_frame = self.get_last_consistent_frame(&newest_generation).await?;
            tracing::info!("Last consistent remote frame: {}", last_consistent_frame);

            if local_change_counter == remote_change_counter {
                let wal_pages = self.get_wal_page_count().await?;
                tracing::warn!("Consistent: {}; wal pages: {}", last_consistent_frame, wal_pages);
                if wal_pages == last_consistent_frame {
                    tracing::warn!(
                        "Newest remote generation is up-to-date, reusing it in this session"
                    );
                    return Ok(RestoreAction::ReuseGeneration(newest_generation));
                }
            }

            let db_file = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&format!("{}/{}.lz4", newest_generation, self.db_name))
                .send()
                .await?;
            // TODO: decompress on the fly, without a separate file
            let compressed_db_path = format!("{}.restored.lz4", self.db_path);
            let mut body_reader = db_file.body.into_async_read();
            let mut compressed_writer = tokio::fs::File::create(&compressed_db_path).await?;
            tokio::io::copy(&mut body_reader, &mut compressed_writer).await?;
            compressed_writer.flush().await?;
            let mut decompressed_reader =
                lz4_flex::frame::FrameDecoder::new(std::fs::File::open(&compressed_db_path)?);
            let mut main_db_writer = std::fs::File::create(&self.db_path)?;
            // FIXME: verify if rewriting the database file is OK during WAL::xOpen
            std::io::copy(&mut decompressed_reader, &mut main_db_writer)?;
            main_db_writer.flush()?;
            tracing::info!("Restored main db file");

            let mut next_marker = None;
            let prefix = format!("{}/", newest_generation);
            tracing::warn!("Overwriting any existing WAL file: {}-wal", &self.db_path);
            tokio::fs::remove_file(&format!("{}-wal", &self.db_path))
                .await
                .ok();
            tokio::fs::remove_file(&format!("{}-shm", &self.db_path))
                .await
                .ok();
            loop {
                let mut list_request = self
                    .client
                    .list_objects()
                    .bucket(&self.bucket)
                    .prefix(&prefix);
                if let Some(marker) = next_marker {
                    list_request = list_request.marker(marker);
                }
                let response = list_request.send().await?;
                let objs = match response.contents() {
                    Some(objs) => objs,
                    None => return Ok(RestoreAction::SnapshotMainDbFile),
                };
                let mut main_db_async_writer = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&self.db_path)
                    .await?;
                //TODO: consider higher concurrency
                for obj in objs {
                    let key = obj
                        .key()
                        .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                    tracing::debug!("Loading {}", key);
                    let frame = self
                        .client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await?;

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
                        break
                    }
                    let mut data = frame.body.into_async_read();
                    main_db_async_writer
                        .seek(tokio::io::SeekFrom::Start(
                            pgno as u64 * Self::PAGE_SIZE as u64,
                        ))
                        .await?;
                    // FIXME: we only need to overwrite with the newest page,
                    // no need to replay the whole WAL
                    tokio::io::copy(&mut data, &mut main_db_async_writer).await?;
                    main_db_async_writer.flush().await?;
                    tracing::info!("Written frame {} as main db page {}", frameno, pgno);
                }
                next_marker = response
                    .is_truncated()
                    .then(|| objs.last().map(|elem| elem.key().unwrap().to_string()))
                    .flatten();
                if next_marker.is_none() {
                    break
                }
            }

            Ok::<RestoreAction, anyhow::Error>(RestoreAction::SnapshotMainDbFile)
        })
    }

    pub fn is_bucket_empty(&self) -> Result<bool> {
        self.runtime.block_on(async {
            let objs = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .send()
                .await?;
            match objs.contents() {
                Some(objs) => Ok::<bool, anyhow::Error>(objs.is_empty()),
                None => Ok::<bool, anyhow::Error>(true),
            }
        })
    }

    pub fn boot(&self, next_marker: Option<String>) -> Result<FetchedResults> {
        tracing::debug!("Bootstrapping from offset {:?}", next_marker);
        self.runtime.block_on(async {
            let mut pages = Vec::new();

            let mut list_request = self.client.list_objects().bucket(&self.bucket).max_keys(2);
            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker);
            }
            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(objs) => objs,
                None => {
                    return Ok(FetchedResults {
                        pages,
                        next_marker: None,
                    })
                }
            };
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                if !key.starts_with(&self.db_name) {
                    tracing::debug!("skipping object {}", key);
                    continue;
                }
                tracing::debug!("Loading {}", key);
                let page = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send()
                    .await?;
                // Format: <db-name>-<page-no>
                match key
                    .rfind('-')
                    .map(|index| key[index + 1..].parse::<i32>().ok())
                {
                    Some(Some(pgno)) => {
                        let data = page.body.collect().await.map(|data| data.into_bytes())?;
                        pages.push((pgno, data));
                    }
                    _ => tracing::error!("Failed to parse page number from key {}", key),
                }
            }
            Ok(FetchedResults {
                pages,
                next_marker: response
                    .is_truncated()
                    .then(|| objs.last().map(|elem| elem.key().unwrap().to_string()))
                    .flatten(),
            })
        })
    }
}

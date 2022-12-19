use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::HashMap;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct Replicator {
    pub client: Client,
    write_buffer: HashMap<u32, (i32, BytesMut)>,

    pub page_size: usize,
    generation: uuid::Uuid,
    pub commits_in_current_generation: u32,
    next_frame: u32,
    pub bucket: String,
    pub db_path: String,
    pub db_name: String,
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
    pub const DEFAULT_PAGE_SIZE: usize = 4096;

    pub async fn new() -> Result<Self> {
        let write_buffer = HashMap::new();
        let mut loader = aws_config::from_env();
        if let Ok(endpoint) = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT") {
            loader = loader.endpoint_resolver(Endpoint::immutable(endpoint.parse()?));
        }
        let bucket =
            std::env::var("LIBSQL_BOTTOMLESS_BUCKET").unwrap_or_else(|_| "bottomless".to_string());
        let client = Client::new(&loader.load().await);
        let generation = Self::generate_generation();
        tracing::debug!("Generation {}", generation);

        match client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => tracing::info!("Bucket {} exists and is accessible", bucket),
            Err(aws_sdk_s3::types::SdkError::ServiceError { err, raw: _ })
                if err.is_not_found() =>
            {
                tracing::info!("Bucket {} not found, recreating", bucket);
                client.create_bucket().bucket(&bucket).send().await?;
            }
            Err(e) => return Err(e.into()),
        }

        Ok(Self {
            client,
            write_buffer,
            bucket,
            page_size: Self::DEFAULT_PAGE_SIZE,
            generation,
            commits_in_current_generation: 0,
            next_frame: 1,
            db_path: String::new(),
            db_name: String::new(),
        })
    }

    // The database can use different page size - as soon as it's known,
    // it should be communicated to the replicator via this call.
    // FIXME: we don't support dynamically changing page size, and SQLite,
    // in general, does. It needs to be checked here.
    pub fn set_page_size(&mut self, page_size: usize) {
        self.page_size = page_size
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
        self.commits_in_current_generation = 0;
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
    pub async fn commit(&mut self) -> Result<()> {
        tracing::debug!("Write buffer size: {}", self.write_buffer.len());
        self.commits_in_current_generation += 1;
        let mut tasks = vec![];
        // FIXME: instead of batches processed in bursts, better to allow X concurrent tasks with a semaphore
        const CONCURRENCY: usize = 16;
        for (frame, (pgno, bytes)) in self.write_buffer.iter() {
            let data: &[u8] = bytes;
            if data.len() != self.page_size {
                tracing::warn!("Unexpected truncated page of size {}", data.len())
            }
            let mut compressor = async_compression::tokio::bufread::GzipEncoder::new(data);
            let mut compressed: Vec<u8> = Vec::with_capacity(self.page_size);
            tokio::io::copy(&mut compressor, &mut compressed).await?;
            let key = format!(
                "{}-{}/{:012}-{:012}",
                self.db_name, self.generation, frame, pgno
            );
            tracing::info!("Committing {} (compressed size: {})", key, compressed.len());
            tasks.push(
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(ByteStream::from(compressed))
                    .send(),
            );
            if tasks.len() >= CONCURRENCY {
                futures::future::try_join_all(std::mem::take(&mut tasks)).await?;
                tasks.clear();
            }
        }
        if !tasks.is_empty() {
            futures::future::try_join_all(tasks).await?;
        }
        self.write_buffer.clear();
        // Last consistent frame is persisted in S3 in order to be able to recover
        // from failured that happen in the middle of a commit, when only some
        // of the pages that belong to a transaction are replicated.
        let last_consistent_frame_key = format!("{}-{}/.consistent", self.db_name, self.generation);
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
        Ok(())
    }

    async fn read_change_counter(reader: &mut tokio::fs::File) -> Result<[u8; 4]> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24)).await?;
        reader.read_exact(&mut counter).await?;
        Ok(counter)
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

    // Replicates local WAL pages to S3, if local WAL is present.
    // This function is called under the assumption that if local WAL
    // file is present, it was already detected to be newer than its
    // remote counterpart.
    pub async fn maybe_replicate_wal(&mut self) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut wal_file = match tokio::fs::File::open(&format!("{}-wal", &self.db_path)).await {
            Ok(file) => file,
            Err(_) => {
                tracing::info!("Local WAL not present - not replicating");
                return Ok(());
            }
        };
        let len = match wal_file.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        };
        if len < 32 {
            tracing::info!("Local WAL is empty, not replicating");
            return Ok(());
        }
        tracing::info!("Local WAL pages: {}", (len - 32) / self.page_size as u64);
        for offset in (32..len).step_by(self.page_size + 24) {
            wal_file.seek(tokio::io::SeekFrom::Start(offset)).await?;
            let pgno = wal_file.read_i32().await?;
            let size_after_transaction = wal_file.read_i32().await?;
            tracing::warn!(
                "Size after transaction for {} is {}",
                pgno,
                size_after_transaction
            );
            wal_file
                .seek(tokio::io::SeekFrom::Start(offset + 24))
                .await?;
            let mut data = vec![0u8; self.page_size];
            wal_file.read_exact(&mut data).await?;
            self.write(pgno, &data);
            // In multi-page transactions, only the last page in the transaction contains
            // the size_after_transaction field. If it's zero, it means it's an uncommited
            // page.
            if size_after_transaction != 0 {
                self.commit().await?;
            }
        }
        if !self.write_buffer.is_empty() {
            tracing::warn!("Uncommited WAL entries: {}", self.write_buffer.len());
        }
        self.write_buffer.clear();
        tracing::info!("Local WAL replicated");
        Ok(())
    }

    // Sends the main database file to S3 - if -wal file is present, it's replicated
    // too - it means that the local file was detected to be newer than its remote
    // counterpart.
    pub async fn snapshot_main_db_file(&mut self) -> Result<()> {
        if !std::path::Path::new(&self.db_path).exists() {
            tracing::info!("Not snapshotting, the main db file does not exist");
            return Ok(());
        }
        tracing::debug!("Snapshotting {}", self.db_path);

        // TODO: find a way to compress ByteStream on the fly instead of creating
        // an intermediary file.
        let (compressed_db_path, change_counter) = self.compress_main_db_file().await?;
        let key = format!("{}-{}/db.gz", self.db_name, self.generation);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from_path(compressed_db_path).await?)
            .send()
            .await?;
        /* FIXME: we can't rely on the change counter in WAL mode:
         ** "In WAL mode, changes to the database are detected using the wal-index and
         ** so the change counter is not needed. Hence, the change counter might not be
         ** incremented on each transaction in WAL mode."
         ** Instead, we need to consult WAL checksums.
         */
        let change_counter_key = format!("{}-{}/.changecounter", self.db_name, self.generation);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(change_counter_key)
            .body(ByteStream::from(Bytes::copy_from_slice(&change_counter)))
            .send()
            .await?;
        tracing::debug!("Main db snapshot complete");
        Ok(())
    }

    //FIXME: assumes that this bucket stores *only* generations,
    // it should be more robust
    pub async fn find_newest_generation(&self) -> Option<uuid::Uuid> {
        let response = self.list_objects().max_keys(1).send().await.ok()?;
        let objs = response.contents()?;
        let key = objs.first()?.key()?;
        let key = match key.find('/') {
            Some(index) => &key[self.db_name.len() + 1..index],
            None => key,
        };
        tracing::info!("Generation candidate: {}", key);
        uuid::Uuid::parse_str(key).ok()
    }

    pub async fn get_remote_change_counter(&self, generation: &uuid::Uuid) -> Result<[u8; 4]> {
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

    pub async fn get_last_consistent_frame(&self, generation: &uuid::Uuid) -> Result<u32> {
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
                (metadata.len() / (self.page_size + 24) as u64) as u32
            }
            Err(_) => 0,
        }
    }

    fn parse_frame_and_page_numbers(key: &str) -> Option<(u32, i32)> {
        // Format: <generation>/<db-name>-<frame-number>-<page-number>
        let page_delim = key.rfind('-')?;
        let frame_delim = key[0..page_delim].rfind('/')?;
        let frameno = key[frame_delim + 1..page_delim].parse::<u32>().ok()?;
        let pgno = key[page_delim + 1..].parse::<i32>().ok()?;
        Some((frameno, pgno))
    }

    pub async fn restore_from(&self, generation: uuid::Uuid) -> Result<RestoreAction> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_change_counter = match tokio::fs::File::open(&self.db_path).await {
            Ok(mut db) => Self::read_change_counter(&mut db).await.unwrap_or([0u8; 4]),
            Err(_) => [0u8; 4],
        };

        let remote_change_counter = self.get_remote_change_counter(&generation).await?;
        tracing::info!(
            "Counters: local={:?}, remote={:?}",
            local_change_counter,
            remote_change_counter
        );

        let last_consistent_frame = self.get_last_consistent_frame(&generation).await?;
        tracing::info!("Last consistent remote frame: {}", last_consistent_frame);

        match local_change_counter.cmp(&remote_change_counter) {
            Ordering::Equal => {
                let wal_pages = self.get_wal_page_count().await;
                tracing::debug!(
                    "Consistent: {}; wal pages: {}",
                    last_consistent_frame,
                    wal_pages
                );
                match wal_pages.cmp(&last_consistent_frame) {
                    Ordering::Equal => {
                        tracing::info!(
                            "Remote generation is up-to-date, reusing it in this session"
                        );
                        return Ok(RestoreAction::ReuseGeneration(generation));
                    }
                    Ordering::Greater => {
                        tracing::info!("Local change counter matches the remote one, but local WAL contains newer data, which needs to be replicated");
                        return Ok(RestoreAction::SnapshotMainDbFile);
                    }
                    Ordering::Less => (),
                }
            }
            Ordering::Greater => {
                tracing::info!("Local change counter is larger than its remote counterpart - a new snapshot needs to be replicated");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
            Ordering::Less => (),
        }

        let db_file = self
            .get_object(format!("{}-{}/db.gz", self.db_name, generation))
            .send()
            .await?;
        let body_reader = db_file.body.into_async_read();
        let mut decompress_reader = async_compression::tokio::bufread::GzipDecoder::new(
            tokio::io::BufReader::new(body_reader),
        );
        tokio::fs::rename(&self.db_path, format!("{}.bottomless.backup", self.db_path))
            .await
            .ok(); // Best effort
        let mut main_db_writer = tokio::fs::File::create(&self.db_path).await?;
        tokio::io::copy(&mut decompress_reader, &mut main_db_writer).await?;
        main_db_writer.flush().await?;
        tracing::info!("Restored main db file");

        let mut next_marker = None;
        let prefix = format!("{}-{}/", self.db_name, generation);
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
                None => {
                    tracing::trace!("No objects found in generation {}", generation);
                    break;
                }
            };
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                tracing::info!("Loading {}", key);
                let frame = self.get_object(key.into()).send().await?;

                let (frameno, pgno) = match Self::parse_frame_and_page_numbers(key) {
                    Some(result) => result,
                    None => {
                        tracing::trace!("Failed to parse frame/page from key {}", key);
                        continue;
                    }
                };
                if frameno > last_consistent_frame {
                    tracing::warn!("Remote log contains frame {} larger than last consistent frame ({}), stopping the restoration",
                                frameno, last_consistent_frame);
                    break;
                }
                let body_reader = frame.body.into_async_read();
                let mut decompress_reader = async_compression::tokio::bufread::GzipDecoder::new(
                    tokio::io::BufReader::new(body_reader),
                );
                let offset = (pgno - 1) as u64 * self.page_size as u64;
                main_db_writer
                    .seek(tokio::io::SeekFrom::Start(offset))
                    .await?;
                // FIXME: we only need to overwrite with the newest page,
                // no need to replay the whole WAL
                let copied = tokio::io::copy(&mut decompress_reader, &mut main_db_writer).await?;
                main_db_writer.flush().await?;
                tracing::info!(
                    "Written frame {} as main db page {} ({} bytes, at offset {})",
                    frameno,
                    pgno,
                    copied,
                    offset,
                );
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

    pub async fn restore(&self) -> Result<RestoreAction> {
        let newest_generation = match self.find_newest_generation().await {
            Some(gen) => gen,
            None => {
                tracing::info!("No generation found, nothing to restore");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
        };

        tracing::info!("Restoring from generation {}", newest_generation);
        self.restore_from(newest_generation).await
    }
}

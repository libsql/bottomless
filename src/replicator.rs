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

    pub(crate) next_frame: u32,
    pub(crate) generation: uuid::Uuid,
    pub(crate) bucket: String,
    pub(crate) db_path: String,
    pub(crate) db_name: String,
}

#[derive(Debug)]
pub struct FetchedResults {
    pub pages: Vec<(i32, Bytes)>,
    pub next_marker: Option<String>,
}

impl Replicator {
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
        let node_id = mac_address::get_mac_address()
            .map(|a| a.map(|a| a.bytes()))
            .unwrap_or(Some([0u8; 6]))
            .unwrap_or([0u8; 6]);
        let generation = uuid::Uuid::now_v1(&node_id);
        Ok(Self {
            client,
            write_buffer,
            runtime,
            bucket,
            next_frame: 1, //FIXME: needs to be loaded from S3
            generation,
            db_path: String::new(),
            db_name: String::new(),
        })
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
    pub fn commit(&mut self) -> Result<()> {
        tracing::info!("Write buffer size: {}", self.write_buffer.len());
        self.runtime.block_on(async {
            let tasks = self.write_buffer.iter().map(|(frame, (pgno, bytes))| {
                let data: &[u8] = bytes;
                if data.len() != Self::PAGE_SIZE {
                    tracing::warn!("Unexpected truncated page of size {}", data.len())
                }
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
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }

    // Sends the main database file to S3
    pub fn snapshot_main_db_file(&mut self) -> Result<()> {
        tracing::error!(
            "TODO: implement this! By sending the whole db file, make ByteStream from a file path"
        );
        Ok(())
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

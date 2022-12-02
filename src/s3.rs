use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use tokio::runtime::{Builder, Runtime};
use tracing::{error, info};

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    write_buffer: HashMap<i64, BytesMut>,
    runtime: Runtime,
}

impl Replicator {
    pub const PAGE_SIZE: usize = 4096;

    pub fn new() -> Result<Self> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
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
        Ok(Self {
            client,
            write_buffer,
            runtime,
        })
    }

    pub fn write(&mut self, offset: i64, data: &[u8]) {
        info!("Write operation: {}:{}", offset, data.len());
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(data);
        self.write_buffer.insert(offset, bytes);
    }

    // Sends the pages participating in a commit to S3
    pub fn commit(&mut self, bucket: &str, prefix: &str) -> Result<()> {
        info!("Write buffer size: {}", self.write_buffer.len());
        self.runtime.block_on(async {
            for (offset, bytes) in &self.write_buffer {
                let data: &[u8] = bytes;
                if data.len() != Self::PAGE_SIZE {
                    return Err(anyhow::anyhow!(
                        "Unexpected write not equal to page size: {}",
                        data.len()
                    ));
                }
                let key = format!("{}-{:012}", prefix, offset / Self::PAGE_SIZE as i64);
                info!("Committing {}", key);
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(data.to_owned()))
                    .send()
                    .await?;
            }
            self.write_buffer.clear();
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }

    pub fn boot(&self, bucket: &str) -> Result<Vec<(i32, Bytes)>> {
        info!("Bootstrapping");
        self.runtime.block_on(async {
            let mut pages = Vec::new();
            let objs = self.client.list_objects().bucket(bucket).send().await?;
            let objs = match objs.contents() {
                Some(objs) => objs,
                None => return Ok(pages),
            };
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                info!("Object {}", key);
                let page = self
                    .client
                    .get_object()
                    .bucket(bucket)
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
                    _ => error!("Failed to parse page number from key {}", key),
                }
            }
            Ok(pages)
        })
    }
}

use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    write_buffer: HashMap<i64, BytesMut>,
    runtime: Runtime,
}

impl Replicator {
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

    pub fn commit(&mut self, bucket: &str, prefix: &str) -> Result<()> {
        info!("Write buffer size: {}", self.write_buffer.len());
        self.runtime.block_on(async {
            // TODO: concurrency
            for (offset, bytes) in &self.write_buffer {
                let data: &[u8] = bytes;
                let key = format!("{}-{}", prefix, offset);
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
}

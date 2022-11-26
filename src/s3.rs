use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    write_buffer: HashMap<i64, BytesMut>,
    runtime: Runtime,
}

impl Replicator {
    pub fn new() -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let write_buffer = HashMap::new();
        let client = runtime.block_on(async {
            Client::new(
                &aws_config::from_env()
                    .endpoint_resolver(Endpoint::immutable(
                        "http://localhost:9000".parse().expect("valid URI"),
                    ))
                    .load()
                    .await,
            )
        });
        Self {
            client,
            write_buffer,
            runtime,
        }
    }

    pub fn write(&mut self, offset: i64, data: &[u8]) {
        info!("Write operation: {}:{}", offset, data.len());
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(data);
        self.write_buffer.insert(offset, bytes);
    }

    pub fn commit(&mut self, bucket: &str, prefix: &str) {
        info!("Write buffer size: {}", self.write_buffer.len());
        self.runtime.block_on(async {
            // TODO: concurrency
            for (offset, bytes) in &self.write_buffer {
                let data: &[u8] = bytes;
                let key = format!("{}-{}:{}", prefix, offset, data.len());
                info!("Committing {}", key);
                self.client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(data.to_owned()))
                    .send()
                    .await
                    .expect("sent");
            }
            self.write_buffer.clear();
        });
    }
}

impl Default for Replicator {
    fn default() -> Self {
        Self::new()
    }
}

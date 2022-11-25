use aws_sdk_s3::presigning::config::PresigningConfig;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use tokio::runtime::{Builder, Runtime};
use tracing::trace;

#[derive(Debug)]
pub struct Replicator {
    client: Client,
    runtime: Runtime,
}

impl Replicator {
    pub fn new() -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
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
        Self { client, runtime }
    }

    pub fn send(&self, bucket: &str, offset: usize, data: &[u8]) {
        let expires_in = std::time::Duration::from_secs(9000);

        let key = format!("{}:{}", offset, data.len());
        trace!(key);
        self.runtime.block_on(async {
            self.client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(data.to_owned()))
                .send()
                .await
                .expect("sent");
        });
    }
}

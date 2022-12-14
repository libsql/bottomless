//use clap::{Parser, Subcommand};

struct Client {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl Client {
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    pub async fn list_generations(&self, db: &str) {
        let mut next_marker = None;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(db);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await.unwrap();
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    println!("No prefixes");
                    return;
                }
            };

            println!(
                "----------------------------------------------------------------------------"
            );
            println!(
                "|                                Generations                               |"
            );
            println!(
                "----------------------------------------------------------------------------"
            );
            println!(
                "|                 uuid                 |             created at            |"
            );
            println!(
                "----------------------------------------------------------------------------"
            );
            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[db.len() + 1..prefix.len() - 1];
                    let uuid = uuid::Uuid::try_parse(prefix).unwrap();
                    let (seconds, nanos) = uuid.get_timestamp().unwrap().to_unix();
                    let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
                    let date =
                        chrono::NaiveDateTime::from_timestamp_opt(seconds as i64, nanos).unwrap();
                    println!("| {} | {:>30} UTC |", uuid, date);
                }
            }
            println!(
                "----------------------------------------------------------------------------"
            );

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut loader = aws_config::from_env();
    if let Ok(endpoint) = std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT") {
        loader =
            loader.endpoint_resolver(aws_sdk_s3::Endpoint::immutable(endpoint.parse().unwrap()));
    }
    let bucket =
        std::env::var("LIBSQL_BOTTOMLESS_BUCKET").unwrap_or_else(|_| "bottomless".to_string());
    let client = Client::new(aws_sdk_s3::Client::new(&loader.load().await), bucket);

    client.list_generations("test.db").await;
}

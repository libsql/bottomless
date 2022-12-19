use anyhow::Result;
use clap::{Parser, Subcommand};

struct Replicator {
    inner: bottomless::replicator::Replicator,
}

impl std::ops::Deref for Replicator {
    type Target = bottomless::replicator::Replicator;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Replicator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn uuid_to_datetime(uuid: &uuid::Uuid) -> chrono::NaiveDateTime {
    let (seconds, nanos) = uuid
        .get_timestamp()
        .map(|ts| ts.to_unix())
        .unwrap_or((0, 0));
    let (seconds, nanos) = (253370761200 - seconds, 999000000 - nanos);
    chrono::NaiveDateTime::from_timestamp_opt(seconds as i64, nanos)
        .unwrap_or(chrono::NaiveDateTime::MIN)
}

impl Replicator {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            inner: bottomless::replicator::Replicator::new().await?,
        })
    }

    async fn print_snapshot_summary(&self, generation: &uuid::Uuid) -> Result<()> {
        match self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(format!("{}-{}/db.gz", self.db_name, generation))
            .object_attributes(aws_sdk_s3::model::ObjectAttributes::ObjectSize)
            .send()
            .await
        {
            Ok(attrs) => {
                println!("\tmain database snapshot:");
                println!("\t\tobject size:   {}", attrs.object_size());
                println!(
                    "\t\tlast modified: {}",
                    attrs
                        .last_modified()
                        .map(|s| s
                            .fmt(aws_smithy_types::date_time::Format::DateTime)
                            .unwrap_or_else(|e| e.to_string()))
                        .as_deref()
                        .unwrap_or("never")
                );
            }
            Err(aws_sdk_s3::types::SdkError::ServiceError(err)) if err.err().is_no_such_key() => {
                println!("\tno main database snapshot file found")
            }
            Err(e) => println!("\tfailed to fetch main database snapshot info: {}", e),
        };
        Ok(())
    }

    pub async fn list_generations(
        &self,
        limit: Option<u64>,
        older_than: Option<chrono::NaiveDate>,
        newer_than: Option<chrono::NaiveDate>,
        verbose: bool,
    ) -> Result<()> {
        let mut next_marker = None;
        let mut limit = limit.unwrap_or(u64::MAX);
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(&self.db_name);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    println!("No prefixes");
                    return Ok(());
                }
            };

            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[self.db_name.len() + 1..prefix.len() - 1];
                    let uuid = uuid::Uuid::try_parse(prefix)?;
                    let datetime = uuid_to_datetime(&uuid);
                    if datetime.date() < newer_than.unwrap_or(chrono::NaiveDate::MIN) {
                        continue;
                    }
                    if datetime.date() > older_than.unwrap_or(chrono::NaiveDate::MAX) {
                        continue;
                    }
                    println!("{}", uuid);
                    if verbose {
                        let counter = self.get_remote_change_counter(&uuid).await?;
                        let consistent_frame = self.get_last_consistent_frame(&uuid).await?;
                        println!("\tcreated at (UTC):     {}", datetime);
                        println!("\tchange counter:       {:?}", counter);
                        println!("\tconsistent WAL frame: {}", consistent_frame);
                        self.print_snapshot_summary(&uuid).await?;
                        println!()
                    }
                }
                limit -= 1;
                if limit == 0 {
                    return Ok(());
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                return Ok(());
            }
        }
    }

    pub async fn remove(&self, generation: uuid::Uuid) -> Result<()> {
        //TODO: Loop over all object with the generation prefix and drop them one by one
        let mut next_marker = None;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .prefix(format!("{}-{}/", &self.db_name, generation));

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(prefixes) => prefixes,
                None => {
                    println!("No objects");
                    return Ok(());
                }
            };

            for obj in objs {
                println!("Stub, but will remove {:?}", obj.key());
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                return Ok(());
            }
        }
    }

    pub async fn list_generation(&self, generation: uuid::Uuid) -> Result<()> {
        self.client
            .list_objects()
            .bucket(&self.bucket)
            .prefix(format!("{}-{}/", &self.db_name, generation))
            .max_keys(1)
            .send()
            .await?
            .contents()
            .ok_or_else(|| {
                anyhow::anyhow!("Generation {} not found for {}", generation, &self.db_name)
            })?;

        let counter = self.get_remote_change_counter(&generation).await?;
        let consistent_frame = self.get_last_consistent_frame(&generation).await?;
        println!("Generation {} for {}", generation, self.db_name);
        println!("\tcreated at:           {}", uuid_to_datetime(&generation));
        println!("\tchange counter:       {:?}", counter);
        println!("\tconsistent WAL frame: {}", consistent_frame);
        self.print_snapshot_summary(&generation).await?;
        Ok(())
    }
}

#[derive(Debug, Parser)]
#[command(name = "bottomless-cli")]
#[command(about = "Bottomless CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[clap(long, short)]
    endpoint: Option<String>,
    #[clap(long, short)]
    database: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[clap(about = "List available generations")]
    Ls {
        #[clap(long, short, long_help = "List details about single generation")]
        generation: Option<uuid::Uuid>,
        #[clap(
            long,
            short,
            conflicts_with = "generation",
            long_help = "List only <limit> newest generations"
        )]
        limit: Option<u64>,
        #[clap(
            long,
            conflicts_with = "generation",
            long_help = "List only generations older than given date"
        )]
        older_than: Option<chrono::NaiveDate>,
        #[clap(
            long,
            conflicts_with = "generation",
            long_help = "List only generations newer than given date"
        )]
        newer_than: Option<chrono::NaiveDate>,
        #[clap(
            long,
            short,
            long_help = "Print detailed information on each generation"
        )]
        verbose: bool,
    },
    #[clap(about = "Restore the database")]
    Restore {
        #[clap(
            long,
            short,
            long_help = "Generation to restore from.\nSkip this parameter to restore from the newest generation."
        )]
        generation: Option<uuid::Uuid>,
    },
    #[clap(about = "Remove given generation from remote storage")]
    Rm {
        #[clap(long, short)]
        generation: uuid::Uuid,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let options = Cli::parse();

    if let Some(ep) = options.endpoint {
        std::env::set_var("LIBSQL_BOTTOMLESS_ENDPOINT", ep)
    }

    let mut client = Replicator::new().await.unwrap();
    client.register_db(options.database);

    match options.command {
        Commands::Ls {
            generation,
            limit,
            older_than,
            newer_than,
            verbose,
        } => match generation {
            Some(gen) => client.list_generation(gen).await.unwrap(),
            None => client
                .list_generations(limit, older_than, newer_than, verbose)
                .await
                .unwrap(),
        },
        Commands::Restore { generation } => {
            match generation {
                Some(gen) => client.restore_from(gen).await.unwrap(),
                None => client.restore().await.unwrap(),
            };
        }
        Commands::Rm { generation } => client.remove(generation).await.unwrap(),
    }
}

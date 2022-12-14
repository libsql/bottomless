# Bottomless S3-compatible virtual WAL for libSQL
##### Work in heavy progress!

This project implements a virtual write-ahead log (WAL) which continuously backs up the data to S3-compatible storage and is able to restore it later.

## How to build
```
make
```
will produce a loadable `.so` libSQL/SQLite extension with bottomless WAL implementation.
```
make release
```
will do the same, but for release mode.

## Configuration
By default, the S3 storage is expected to be available at `http://localhost:9000` (e.g. a local development [minio](https://min.io) server), and the auth information is extracted via regular S3 SDK mechanisms, i.e. environment variables and `~/.aws/credentials` file, if present. Ref: https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_environment.html

Default endpoint can be overridden by an environment variable too, and in the future it will be available directly from libSQL as an URI parameter:
```
export LIBSQL_BOTTOMLESS_ENDPOINT='http://localhost:9042'
```

Bucket used for replication can be configured with:
```
export LIBSQL_BOTTOMLESS_BUCKET='http://localhost:9042'
```

On top of that, bottomless is implemented on top of the official [Rust SDK for S3](https://crates.io/crates/aws-sdk-s3), so all AWS-specific environment variables like `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` also work, as well as the `~/.aws/credentials` file.

## How to use
From libSQL/SQLite shell, load the extension and open a database file with `bottomless` WAL, e.g.:
```sql
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA journal_mode=wal;
```
Remember to set the journaling mode to `WAL`, which needs to be done at least once, before writing any content, otherwise the custom WAL implementation will not be used.

In order to customize logging, use `RUST_LOG` env variable, e.g. `RUST_LOG=info ./libsql`.

A short demo script is in `test/smoke_test.sh`, and can be executed with:

```sh
make test
```

## Details
All page writes committed to the database end up being synchronously replicated to S3-compatible storage.
On boot, if the main database file is empty, it will be restored with data coming from the remote storage.

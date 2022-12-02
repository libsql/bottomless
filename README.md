# Bottomless S3-compatible VFS for libSQL/SQLite
Work in heavy progress!

## How to build
```
make
```
will produce a loadable `.so` libSQL/SQLite extension with bottomless VFS.
```
make debug
```
will do the same, but for debug mode.

## Configuration
By default, the S3 storage is expected to be available at `http://localhost:9000` (e.g. a local development [minio](https://min.io) server), and the auth information is extracted via regular S3 SDK mechanisms, i.e. environment variables and `~/.aws/credentials` file, if present. Ref: https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_environment.html

Default endpoint can be overridden by an environment variable too, and in the future it will be available directly from libSQL as an URI parameter:
```
export LIBSQL_BOTTOMLESS_ENDPOINT='http://localhost:9042'
```

Buckets are not configurable yet.

## How to use
From libSQL/SQLite shell, load the extension and open a database file with `bottomless` VFS, e.g.:
```sql
.load ../target/release/bottomless
.open file:test.db?vfs=bottomless
```

In order to customize logging, use `RUST_LOG` env variable, e.g. `RUST_LOG=info ./libsql`.

A short demo script is available in the `test/` directory. It assumes that a local S3-compatible server runs at `http://localhost:9000`.
```sh
cd test
./smoke_test.sh
```

## Details
All page writes performed by the database end up being synchronously replicated to S3-compatible storage.
On boot, if the main database file is empty, it will be filled with data coming from S3.
The bucket is currently hardcoded to `libsql`, and pages are stored in the following format `<db-name>-<page-number>`.


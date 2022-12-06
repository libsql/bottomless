# Bottomless S3-compatible VFS for libSQL/SQLite
Work in heavy progress!

## How to build
```
make
```
will produce a loadable `.so` libSQL/SQLite extension with bottomless VFS.
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

## Initial replication
Once the bottomless module is loaded, it will try to replicate the database file to S3, if the database file is not empty.
However, if the target bucket is also non-empty, there's a potential conflict.

The default behavior is to error out in this case, however there are two alternatives:
 - `force`
    the initial replication is performed, possibly overwriting the contents of the target bucket
 - `skip`
    the initial replication is not performed, and the local database file is treated as the source of truth

The initial replication policy can be set with the following environment variable:
```
LIBSQL_BOTTOMLESS_INITIAL_REPLICATION=force
```
or
```
LIBSQL_BOTTOMLESS_INITIAL_REPLICATION=skip
```

## Details
All page writes performed by the database end up being synchronously replicated to S3-compatible storage.
On boot, if the main database file is empty, it will be filled with data coming from S3.
The bucket is currently hardcoded to `libsql`, and pages are stored in the following format `<db-name>-<page-number>`.


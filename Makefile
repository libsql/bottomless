all:	debug

debug:	prep debug_standalone

debug_standalone:	bottomless.c core/src/lib.rs
	cargo build -p bottomless && clang -Wall -fPIC -shared -Ilibsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so

release:	prep release_standalone

release_standalone:	bottomless.c core/src/lib.rs
	cargo build -p bottomless -j1	--quiet --release && \
		clang -fPIC -shared -I libsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/release/libbottomless.a \
		-o target/release/bottomless.so

debug_sync:	prep debug_sync_standalone
	cargo build -p bottomless --no-default-features -F sync && clang -Wall -fPIC -shared -Ilibsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so

debug_sync_standalone:	bottomless.c core/src/lib.rs
	cargo build -p bottomless --no-default-features -F sync && clang -Wall -fPIC -shared -Ilibsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so


prep:
	( cd libsql && make || ( ./configure && make ) )

.PHONY: test
test:	debug prep
	( cd test && ./smoke_test.sh )

.PHONY: test_sync
test_sync:	debug_sync prep
	( cd test && ./smoke_test.sh )

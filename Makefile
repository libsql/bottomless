all:	debug

debug:	prep debug_standalone

debug_standalone:	bottomless.c core/src/lib.rs
	cargo build && clang -Wall -fPIC -shared -Ilibsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so

release:	prep release_standalone

release_standalone:	bottomless.c core/src/lib.rs
	cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort -j1 \
		--quiet --release --target x86_64-unknown-linux-gnu && \
		clang -fPIC -shared -I libsql/ -DLIBSQL_ENABLE_BOTTOMLESS_WAL bottomless.c target/x86_64-unknown-linux-gnu/release/libbottomless.a \
		-o target/x86_64-unknown-linux-gnu/release/bottomless.so

prep:
	( cd libsql && make || ( ./configure && make ) )

.PHONY: test
test:	debug prep
	( cd test && ./smoke_test.sh )

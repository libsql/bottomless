all:	debug

debug:	prep bottomless.c core/src/lib.rs
	cargo build && gcc -Wall -fPIC -shared bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so

release:	prep bottomless.c core/src/lib.rs
	cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort -j1 \
		--quiet --release --target x86_64-unknown-linux-gnu && \
		gcc -fPIC -shared bottomless.c target/x86_64-unknown-linux-gnu/release/libbottomless.a \
		-o target/x86_64-unknown-linux-gnu/release/bottomless.so

prep:
	( cd libsql && make || ( ./configure && make ) )

.PHONY: test
test:	debug prep
	( cd test && ./restore_test.sh )

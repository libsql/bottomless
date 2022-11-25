all:	bottomless.c src/lib.rs
	cargo build --release && gcc -fPIC -shared bottomless.c target/release/libbottomless.a -o target/release/bottomless.so

debug:	bottomless.c src/lib.rs
	cargo build && gcc -fPIC -shared bottomless.c target/debug/libbottomless.a -o target/debug/bottomless.so

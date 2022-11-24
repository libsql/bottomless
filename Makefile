all:	bottomless.c src/lib.rs
	cargo build --release && gcc -fPIC -shared bottomless.c target/release/libbottomless.a -o target/release/bottomless.so

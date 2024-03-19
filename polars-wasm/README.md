# polars-wasm

A `describe()` function for wasm.

## build
```sh
cargo install wasm-pack # install wasm-pack
wasm-pack build --release # build wasm files in ./pkg

cd frontend
pnpm i # install node deps incl the wasm pkg
pnpm run dev # start dev server
```

import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";


export default defineConfig({
  server: {
    port: 4000,
    strictPort: true,
  },
  plugins: [wasm(), topLevelAwait()],
//   worker: {
//     plugins: [wasm(), topLevelAwait()],
//   },
});

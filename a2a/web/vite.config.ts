/// <reference types="vitest" />
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
  },
  test: {
    // Node by default; the component tests opt into jsdom with a
    // `@vitest-environment jsdom` docblock.
    environment: "node",
  },
});

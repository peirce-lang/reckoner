import { defineConfig } from 'vite'

export default defineConfig({
  base: './',
  build: {
    outDir: 'Reckoner/webroot/reckoner',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      }
    }
  }
})
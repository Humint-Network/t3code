import { describe, expect, it } from "vite-plus/test";

import { localEmbeddingHeaders } from "./localEmbedding.ts";

describe("supervised local embedding", () => {
  it("allows only the exact local parent on an explicitly loopback-bound server", () => {
    expect(localEmbeddingHeaders("http://127.0.0.1:8765", "127.0.0.1")).toEqual({
      "Content-Security-Policy": "frame-ancestors http://127.0.0.1:8765",
    });
  });

  it("retains frame denial for remote, wildcard, credential and malformed configurations", () => {
    for (const ancestor of [
      undefined,
      "",
      "*",
      "http://localhost:8765",
      "https://example.com",
      "http://127.0.0.1:8765/path",
      "http://127.0.0.1:8765#x",
      "http://127.0.0.1:8765?x",
      "http://user@127.0.0.1:8765",
      "http://127.0.0.1",
      "http://127.0.0.1:8765 http://evil.test",
      "http://127.0.0.1.evil.test:8765",
      "http://127.0.0.1:8765; script-src *",
    ]) {
      expect(localEmbeddingHeaders(ancestor, "127.0.0.1")).toEqual({
        "Content-Security-Policy": "frame-ancestors 'none'",
        "X-Frame-Options": "DENY",
      });
    }
    for (const host of [undefined, "0.0.0.0", "localhost", "192.168.1.3"]) {
      expect(localEmbeddingHeaders("http://127.0.0.1:8765", host)["X-Frame-Options"]).toBe("DENY");
    }
  });
});

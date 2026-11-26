export const localEmbeddingHeaders = (ancestor: string | undefined, host: string | undefined) => {
  if (host === "127.0.0.1" && ancestor !== undefined) {
    try {
      const url = new URL(ancestor);
      if (
        url.protocol === "http:" &&
        url.hostname === "127.0.0.1" &&
        url.port !== "" &&
        url.origin === ancestor &&
        url.username === "" &&
        url.password === ""
      ) {
        return {
          "Content-Security-Policy":
            "img-src 'self' data: blob:; media-src 'self' blob:; font-src 'self' data:; " +
            `object-src 'none'; frame-ancestors ${url.origin}`,
        };
      }
    } catch {
      // Invalid supervisor configuration retains the normal frame denial.
    }
  }
  return { "Content-Security-Policy": "frame-ancestors 'none'", "X-Frame-Options": "DENY" };
};

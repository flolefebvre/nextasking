import type { NextConfig } from "next";

const standaloneConfig: NextConfig = {
  output: "standalone",
  outputFileTracingIncludes: {
    "/*": ["./node_modules/@flefebvre/nextasking/package.json", "./node_modules/@flefebvre/nextasking/dist/bin.js"],
  },
};

const nextConfig: NextConfig = process.env.NEXTASKING_E2E_STANDALONE === "1" ? standaloneConfig : {};

export default nextConfig;

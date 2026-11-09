import type { NextConfig } from "next";

const nextConfig: NextConfig = process.env.NEXTASKING_E2E_STANDALONE === "1" ? { output: "standalone" } : {};

export default nextConfig;

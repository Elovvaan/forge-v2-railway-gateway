diff --git a/server.js b/server.js
index b2285cb0d380d1e275332197ceea530df26ed1ae..89438ede2f3fbe2a1481e86cda3d230962e0b354 100644
--- a/server.js
+++ b/server.js
@@ -1,63 +1,93 @@
 import express from "express";
 import cors from "cors";
 import fs from "fs/promises";
 import path from "path";
 import os from "os";
 import crypto from "crypto";
 
 const app = express();
 app.use(cors());
 app.use(express.json({ limit: process.env.EXPRESS_JSON_LIMIT || "25mb" }));
 
 const PORT = process.env.PORT || 3000;
 const GATEWAY_TOKEN = process.env.FORGE_GATEWAY_TOKEN || "";
 const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
 const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";
+const DEFAULT_CHUNK_BYTES = (() => {
+  const parsed = Number.parseInt(process.env.DEFAULT_CHUNK_BYTES || "", 10);
+  return parsed > 0 ? parsed : 1048576;
+})();
 const MAX_CHUNK_BYTES = (() => {
   const parsed = Number.parseInt(process.env.MAX_CHUNK_BYTES || "", 10);
-  return parsed > 0 ? parsed : 20 * 1024 * 1024;
+  return parsed > 0 ? parsed : 2097152;
 })();
 const MAX_FILE_BYTES = (() => {
   const parsed = Number.parseInt(process.env.MAX_FILE_BYTES || "", 10);
   return parsed > 0 ? parsed : 2 * 1024 * 1024 * 1024;
 })();
 const JOB_TTL_MS = (() => {
   const parsed = Number.parseInt(process.env.JOB_TTL_MS || "", 10);
   return parsed > 0 ? parsed : 2 * 60 * 60 * 1000;
 })();
 
 const uploadDir = path.join(os.tmpdir(), "forge-gateway-chunked");
 const jobStateDir = path.join(os.tmpdir(), "forge-gateway-jobs");
 const jobs = new Map();
 // Per-job write chains: serializes concurrent appendFile calls to prevent interleaved writes
 const jobWriteChains = new Map();
 
 function nowIso() {
   return new Date().toISOString();
 }
 
+function delay(ms) {
+  return new Promise((resolve) => setTimeout(resolve, ms));
+}
+
+async function appendChunkWithRetry({ jobId, filepath, chunk }) {
+  const maxAttempts = 3;
+  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
+    try {
+      await fs.appendFile(filepath, chunk);
+      return;
+    } catch (error) {
+      if (attempt === maxAttempts) {
+        throw error;
+      }
+      const retryCount = attempt;
+      console.warn("Chunk append failed; retrying", {
+        jobId,
+        retry_count: retryCount,
+        max_retries: maxAttempts - 1,
+        error: error instanceof Error ? error.message : String(error)
+      });
+      await delay(2000);
+    }
+  }
+}
+
 async function persistJob(job) {
   try {
     await fs.mkdir(jobStateDir, { recursive: true });
     const serializable = {
       ...job,
       chunk_offsets: job.chunk_offsets ? Array.from(job.chunk_offsets) : []
     };
     await fs.writeFile(path.join(jobStateDir, `${job.job_id}.json`), JSON.stringify(serializable), "utf8");
   } catch {
     // best-effort; in-memory state is authoritative
   }
 }
 
 async function removeJobState(jobId) {
   try {
     await fs.unlink(path.join(jobStateDir, `${jobId}.json`));
   } catch {
     // no-op if already absent
   }
 }
 
 async function recoverJobs() {
   try {
     await fs.mkdir(jobStateDir, { recursive: true });
     const files = await fs.readdir(jobStateDir);
@@ -232,125 +262,134 @@ app.get("/health", (_req, res) => {
   });
 });
 
 app.post("/providers/test", requireGatewayToken, (_req, res) => {
   res.json({
     ok: true,
     railway_env_present: {
       GEMINI_API_KEY: Boolean(process.env.GEMINI_API_KEY),
       FORGE_GATEWAY_TOKEN: Boolean(process.env.FORGE_GATEWAY_TOKEN),
       GEMINI_MODEL: process.env.GEMINI_MODEL || "gemini-2.5-flash"
     },
     message: "Chunked async Gemini gateway is enabled."
   });
 });
 
 app.post("/video/start", requireGatewayToken, async (req, res) => {
   try {
     const { filename = "upload.mp4", mimetype = "video/mp4" } = req.body || {};
 
     if (typeof filename !== "string" || filename.trim() === "") {
       return res.status(400).json({ ok: false, error: "Invalid filename", message: "filename must be a non-empty string." });
     }
     if (typeof mimetype !== "string" || mimetype.trim() === "") {
       return res.status(400).json({ ok: false, error: "Invalid mimetype", message: "mimetype must be a non-empty string." });
     }
-    
+
     const jobId = `forge_video_${Date.now()}_${crypto.randomUUID()}`;
     await ensureUploadDir();
 
     const filepath = path.join(uploadDir, `${jobId}${path.extname(filename) || ".mp4"}`);
     await fs.writeFile(filepath, Buffer.alloc(0));
 
     const newJob = {
       job_id: jobId,
       status: "uploading",
       created_at: nowIso(),
       filepath,
       filename,
       mimetype,
       chunks_received: 0,
       bytes_received: 0,
       expected_chunks: null,
       chunk_offsets: new Set(),
       pending_appends: 0,
       result: null,
       error: null
     };
     jobs.set(jobId, newJob);
     jobWriteChains.set(jobId, Promise.resolve());
     await persistJob(newJob);
 
-    res.json({ ok: true, job_id: jobId, status: "uploading", chunk_upload_url: `/video/chunk/${jobId}`, complete_url: `/video/complete/${jobId}`, result_url: `/video/result/${jobId}` });
+    res.json({
+      ok: true,
+      job_id: jobId,
+      status: "uploading",
+      chunk_size_bytes: DEFAULT_CHUNK_BYTES,
+      max_chunk_bytes: MAX_CHUNK_BYTES,
+      chunk_upload_url: `/video/chunk/${jobId}`,
+      complete_url: `/video/complete/${jobId}`,
+      result_url: `/video/result/${jobId}`
+    });
   } catch (error) {
     res.status(500).json({ ok: false, error: "Internal server error", message: error instanceof Error ? error.message : String(error) });
   }
 });
 
 app.post("/video/chunk/:job_id", requireGatewayToken, express.raw({ type: "application/octet-stream", limit: `${MAX_CHUNK_BYTES}b` }), async (req, res) => {
   const jobId = String(req.params.job_id);
   try {
     const job = jobs.get(jobId);
     if (!job) return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });
     if (job.status !== "uploading") return res.status(409).json({ ok: false, error: "Job is not accepting chunks", status: job.status });
 
     const chunk = req.body;
     if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
       return res.status(400).json({ ok: false, error: "Missing chunk body. Send raw application/octet-stream." });
     }
 
     const offsetHeader = req.headers["x-chunk-offset"];
     const offset = offsetHeader ? Number.parseInt(String(offsetHeader), 10) : null;
 
     if (offset === null || !Number.isInteger(offset) || offset < 0) {
       return res.status(400).json({ ok: false, error: "Missing or invalid x-chunk-offset header", message: "Send a non-negative integer offset." });
     }
 
     if (job.chunk_offsets.has(offset)) {
       return res.json({ ok: true, job_id: jobId, status: job.status, chunks_received: job.chunks_received, bytes_received: job.bytes_received, message: "Chunk already received (idempotent)." });
     }
 
     const chunkSize = chunk.length;
     if (job.bytes_received + chunkSize > MAX_FILE_BYTES) {
       return res.status(413).json({
         ok: false,
         error: "Upload size limit exceeded",
         message: `File would exceed the ${MAX_FILE_BYTES}-byte maximum.`,
         bytes_received: job.bytes_received,
         max_file_bytes: MAX_FILE_BYTES
       });
     }
 
     job.pending_appends += 1;
     jobs.set(jobId, job);
 
     // Serialize this write through the per-job chain so concurrent requests don't interleave bytes
     const prevChain = jobWriteChains.get(jobId) ?? Promise.resolve();
-    const thisWrite = prevChain.then(() => fs.appendFile(job.filepath, chunk));
+    const thisWrite = prevChain.then(() => appendChunkWithRetry({ jobId, filepath: job.filepath, chunk }));
     jobWriteChains.set(jobId, thisWrite.catch(() => {}));
     await thisWrite;
-    
+
     job.chunk_offsets.add(offset);
     job.chunks_received += 1;
     job.bytes_received += chunkSize;
     job.pending_appends -= 1;
     jobs.set(jobId, job);
     await persistJob(job);
 
     return res.json({ ok: true, job_id: jobId, status: job.status, chunks_received: job.chunks_received, bytes_received: job.bytes_received });
   } catch (error) {
     const job = jobs.get(jobId);
     if (job && job.pending_appends > 0) {
       job.pending_appends -= 1;
       jobs.set(jobId, job);
     }
     res.status(500).json({ ok: false, error: "Internal server error", message: error instanceof Error ? error.message : String(error) });
   }
 });
 
 app.post("/video/complete/:job_id", requireGatewayToken, async (req, res) => {
   try {
     const jobId = req.params.job_id;
     const job = jobs.get(jobId);
     if (!job) return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });
     if (job.status !== "uploading") return res.status(409).json({ ok: false, error: "Job already completed or invalid state", status: job.status });
 

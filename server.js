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
const DEFAULT_CHUNK_BYTES = (() => {
  const parsed = Number.parseInt(process.env.DEFAULT_CHUNK_BYTES || "", 10);
  return parsed > 0 ? parsed : 1048576;
})();
const MAX_CHUNK_BYTES = (() => {
  const parsed = Number.parseInt(process.env.MAX_CHUNK_BYTES || "", 10);
  return parsed > 0 ? parsed : 2097152;
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function appendChunkWithRetry({ jobId, filepath, chunk }) {
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await fs.appendFile(filepath, chunk);
      return;
    } catch (error) {
      if (attempt === maxAttempts) {
        throw error;
      }
      console.warn("Chunk append failed; retrying", {
        jobId,
        retry_count: attempt,
        max_attempts: maxAttempts,
        error: error instanceof Error ? error.message : String(error)
      });
      await delay(2000);
    }
  }
}

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
    for (const file of files) {
      if (!file.endsWith(".json")) continue;
      try {
        const raw = await fs.readFile(path.join(jobStateDir, file), "utf8");
        const job = JSON.parse(raw);

        // Restore Set from array
        if (Array.isArray(job.chunk_offsets)) {
          job.chunk_offsets = new Set(job.chunk_offsets);
        } else {
          job.chunk_offsets = new Set();
        }

        // Ensure new fields exist
        if (typeof job.pending_appends !== "number") {
          job.pending_appends = 0;
        }

        if (["uploading", "queued", "processing"].includes(job.status)) {
          job.status = "analysis_failed";
          job.error = "Gateway restarted while job was in progress.";
          job.completed_at = nowIso();
          try { await fs.unlink(job.filepath); } catch { /* orphaned file gone or missing */ }
          await persistJob(job);
        }
        jobs.set(job.job_id, job);
      } catch {
        // corrupt state file — skip
      }
    }
  } catch {
    // state dir unreadable — start fresh
  }
}

async function pruneExpiredJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs.entries()) {
    const age = now - new Date(job.created_at).getTime();
    if (age > JOB_TTL_MS) {
      jobs.delete(jobId);
      await removeJobState(jobId);
      try {
        await fs.unlink(job.filepath);
      } catch (err) {
        if (err.code !== "ENOENT") {
          console.error("Failed to unlink expired job temp file", { jobId, filepath: job.filepath, error: err.message });
        }
      }
      jobWriteChains.delete(jobId);
    }
  }
}

function requireGatewayToken(req, res, next) {
  if (!GATEWAY_TOKEN) return next();

  const auth = req.headers.authorization || "";
  const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
  const headerToken = req.headers["x-forge-token"];
  const token = bearer || headerToken;

  if (token !== GATEWAY_TOKEN) {
    return res.status(401).json({ ok: false, error: "Unauthorized", message: "Missing or invalid Forge gateway token." });
  }

  return next();
}

function createEmptyAnalysis() {
  return {
    story_summary: "",
    scene_list: [],
    characters: [],
    locations: [],
    visual_style: "",
    emotional_beats: [],
    unresolved_hooks: [],
    next_episode_outline: "",
    next_scene_prompts: []
  };
}

function safeParseGeminiJson(text) {
  const normalized = text.trim().replace(/^```json\s*/i, "").replace(/^```/, "").replace(/```$/, "").trim();
  try {
    return JSON.parse(normalized);
  } catch (error) {
    const preview = normalized.length > 500 ? `${normalized.slice(0, 500)}...` : normalized;
    throw new Error(
      `Gemini returned invalid JSON: ${error instanceof Error ? error.message : String(error)}. Response preview: ${preview}`,
      { cause: error }
    );
  }
}

async function ensureUploadDir() {
  await fs.mkdir(uploadDir, { recursive: true });
}

async function runGeminiVideoAnalysis(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  try {
    if (!GEMINI_API_KEY) throw new Error("GEMINI_API_KEY is not configured.");

    job.status = "processing";
    job.processing_started_at = nowIso();
    jobs.set(jobId, job);
    await persistJob(job);

    const fileBuffer = await fs.readFile(job.filepath);
    const videoBase64 = fileBuffer.toString("base64");

    const prompt = `Analyze this episode video and return ONLY valid JSON with exactly these top-level keys:\n\nstory_summary (string)\nscene_list (array)\ncharacters (array)\nlocations (array)\nvisual_style (string)\nemotional_beats (array)\nunresolved_hooks (array)\nnext_episode_outline (string)\nnext_scene_prompts (array)\n\nBe concise but complete. No markdown, no code fences.`;

    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;

    const geminiResponse = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }, { inline_data: { mime_type: job.mimetype || "video/mp4", data: videoBase64 } }] }],
        generationConfig: { temperature: 0.3, responseMimeType: "application/json" }
      })
    });

    const payload = await geminiResponse.json();
    if (!geminiResponse.ok) throw new Error(`Gemini API HTTP ${geminiResponse.status}: ${JSON.stringify(payload)}`);

    const text = payload?.candidates?.[0]?.content?.parts?.map((p) => p.text || "").join("\n") || "";
    if (!text) throw new Error("Gemini returned empty content.");

    const parsed = safeParseGeminiJson(text);
    job.result = { ...createEmptyAnalysis(), ...parsed };
    job.status = "analysis_complete";
    job.completed_at = nowIso();
    jobs.set(jobId, job);
    await persistJob(job);
  } catch (error) {
    job.status = "analysis_failed";
    job.error = error instanceof Error ? error.message : String(error);
    job.completed_at = nowIso();
    jobs.set(jobId, job);
    await persistJob(job);
  } finally {
    jobWriteChains.delete(jobId);
    try {
      await fs.unlink(job.filepath);
    } catch (error) {
      if (error?.code !== "ENOENT") {
        console.error("Failed to delete analyzed video file", {
          jobId,
          filepath: job.filepath,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
  }
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "forge-gateway",
    version: "v3-chunked-async-gemini",
    status: "online"
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

    res.json({
      ok: true,
      job_id: jobId,
      status: "uploading",
      chunk_size_bytes: DEFAULT_CHUNK_BYTES,
      max_chunk_bytes: MAX_CHUNK_BYTES,
      chunk_upload_url: `/video/chunk/${jobId}`,
      complete_url: `/video/complete/${jobId}`,
      result_url: `/video/result/${jobId}`
    });
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
    const thisWrite = prevChain.then(() => appendChunkWithRetry({ jobId, filepath: job.filepath, chunk }));
    jobWriteChains.set(jobId, thisWrite.catch(() => {}));
    await thisWrite;

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

    if (job.bytes_received === 0) {
      return res.status(400).json({ ok: false, error: "Cannot complete empty upload", message: "No chunks have been uploaded." });
    }

    if (job.pending_appends > 0) {
      return res.status(409).json({ ok: false, error: "Upload still in progress", message: `${job.pending_appends} chunk(s) still being written. Retry after a short delay.` });
    }

    job.status = "queued";
    job.upload_completed_at = nowIso();
    jobs.set(jobId, job);
    await persistJob(job);

    setImmediate(() => {
      runGeminiVideoAnalysis(jobId);
    });

    return res.json({ ok: true, job_id: jobId, status: "queued", result_url: `/video/result/${jobId}` });
  } catch (error) {
    res.status(500).json({ ok: false, error: "Internal server error", message: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/video/result/:job_id", requireGatewayToken, (req, res) => {
  const jobId = req.params.job_id;
  const job = jobs.get(jobId);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });

  if (["uploading", "queued", "processing"].includes(job.status)) {
    return res.json({ ok: true, job_id: jobId, status: job.status, created_at: job.created_at, processing_started_at: job.processing_started_at || null });
  }

  if (job.status === "analysis_failed") {
    return res.json({ ok: true, job_id: jobId, status: "analysis_failed", error: job.error, completed_at: job.completed_at });
  }

  return res.json({ ok: true, job_id: jobId, status: "analysis_complete", completed_at: job.completed_at, ...job.result });
});

app.use((err, req, res, next) => {
  if (err.type === "entity.too.large") {
    return res.status(413).json({
      ok: false,
      error: "Payload too large",
      message: `Chunk exceeds the ${MAX_CHUNK_BYTES}-byte limit.`,
      max_chunk_bytes: MAX_CHUNK_BYTES
    });
  }
  if (err.status === 400 && err.type) {
    return res.status(400).json({
      ok: false,
      error: "Bad request",
      message: err.message || "Invalid request body."
    });
  }
  console.error("Unhandled error:", err);
  res.status(500).json({ ok: false, error: "Internal server error", message: err.message || String(err) });
});

await recoverJobs();
const pruneInterval = setInterval(pruneExpiredJobs, 60 * 60 * 1000);
pruneInterval.unref();

app.listen(PORT, () => {
  console.log(`forge-gateway listening on port ${PORT}`);
});

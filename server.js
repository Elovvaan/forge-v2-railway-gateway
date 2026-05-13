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
const MAX_CHUNK_BYTES = Number.parseInt(process.env.MAX_CHUNK_BYTES || "", 10) || 20 * 1024 * 1024;

const uploadDir = path.join(os.tmpdir(), "forge-gateway-chunked");
const jobs = new Map();

function nowIso() {
  return new Date().toISOString();
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
      error instanceof Error ? { cause: error } : undefined
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
  } catch (error) {
    job.status = "analysis_failed";
    job.error = error instanceof Error ? error.message : String(error);
    job.completed_at = nowIso();
    jobs.set(jobId, job);
  } finally {
    try {
      await fs.unlink(job.filepath);
    } catch {
      // no-op
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
  const { filename = "upload.mp4", mimetype = "video/mp4" } = req.body || {};
  const jobId = `forge_video_${Date.now()}_${crypto.randomUUID()}`;
  await ensureUploadDir();

  const filepath = path.join(uploadDir, `${jobId}${path.extname(filename) || ".mp4"}`);
  await fs.writeFile(filepath, Buffer.alloc(0));

  jobs.set(jobId, {
    job_id: jobId,
    status: "uploading",
    created_at: nowIso(),
    filepath,
    filename,
    mimetype,
    chunks_received: 0,
    bytes_received: 0,
    result: null,
    error: null
  });

  res.json({ ok: true, job_id: jobId, status: "uploading", chunk_upload_url: `/video/chunk/${jobId}`, complete_url: `/video/complete/${jobId}`, result_url: `/video/result/${jobId}` });
});

app.post("/video/chunk/:job_id", requireGatewayToken, express.raw({ type: "application/octet-stream", limit: `${MAX_CHUNK_BYTES}b` }), async (req, res) => {
  const jobId = req.params.job_id;
  const job = jobs.get(jobId);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });
  if (job.status !== "uploading") return res.status(409).json({ ok: false, error: "Job is not accepting chunks", status: job.status });

  const chunk = req.body;
  if (!chunk || !Buffer.isBuffer(chunk) || chunk.length === 0) {
    return res.status(400).json({ ok: false, error: "Missing chunk body. Send raw application/octet-stream." });
  }

  await fs.appendFile(job.filepath, chunk);
  job.chunks_received += 1;
  job.bytes_received += chunk.length;
  jobs.set(jobId, job);

  return res.json({ ok: true, job_id: jobId, status: job.status, chunks_received: job.chunks_received, bytes_received: job.bytes_received });
});

app.post("/video/complete/:job_id", requireGatewayToken, async (req, res) => {
  const jobId = req.params.job_id;
  const job = jobs.get(jobId);
  if (!job) return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });
  if (job.status !== "uploading") return res.status(409).json({ ok: false, error: "Job already completed or invalid state", status: job.status });

  job.status = "queued";
  job.upload_completed_at = nowIso();
  jobs.set(jobId, job);

  setImmediate(() => {
    runGeminiVideoAnalysis(jobId);
  });

  return res.json({ ok: true, job_id: jobId, status: "queued", result_url: `/video/result/${jobId}` });
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

app.listen(PORT, () => {
  console.log(`forge-gateway listening on port ${PORT}`);
});

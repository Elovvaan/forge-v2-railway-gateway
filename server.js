import express from "express";
import cors from "cors";
import multer from "multer";
import fs from "fs/promises";
import path from "path";
import os from "os";
import crypto from "crypto";

const app = express();

app.use(cors());
app.use(express.json({ limit: "25mb" }));

const PORT = process.env.PORT || 3000;
const GATEWAY_TOKEN = process.env.FORGE_GATEWAY_TOKEN || "";
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";

const uploadDir = path.join(os.tmpdir(), "forge-gateway-uploads");
const ACTIVE_JOB_TTL_MS = 60 * 60 * 1000;
const TERMINAL_JOB_TTL_MS = 10 * 60 * 1000;
const JOB_CLEANUP_INTERVAL_MS = 60 * 1000;

function isTerminalJobState(job) {
  if (!job || typeof job !== "object") return false;

  const status = typeof job.status === "string" ? job.status.toLowerCase() : "";
  return status === "completed" || status === "failed" || status === "error" || status === "cancelled";
}

function createPrunableJobsMap() {
  const map = new Map();
  const expirations = new Map();

  function ttlForValue(value) {
    return isTerminalJobState(value) ? TERMINAL_JOB_TTL_MS : ACTIVE_JOB_TTL_MS;
  }

  function touch(key, value) {
    expirations.set(key, Date.now() + ttlForValue(value));
  }

  function pruneExpired(now = Date.now()) {
    for (const [key, expiresAt] of expirations) {
      if (expiresAt <= now) {
        expirations.delete(key);
        map.delete(key);
      }
    }
  }

  const proxy = new Proxy(map, {
    get(target, prop, receiver) {
      if (prop === "set") {
        return (key, value) => {
          pruneExpired();
          target.set(key, value);
          touch(key, value);
          return receiver;
        };
      }

      if (prop === "get") {
        return (key) => {
          pruneExpired();
          const value = target.get(key);
          if (value !== undefined) {
            touch(key, value);
          } else {
            expirations.delete(key);
          }
          return value;
        };
      }

      if (prop === "has") {
        return (key) => {
          pruneExpired();
          return target.has(key);
        };
      }

      if (prop === "delete") {
        return (key) => {
          expirations.delete(key);
          return target.delete(key);
        };
      }

      if (prop === "clear") {
        return () => {
          expirations.clear();
          target.clear();
        };
      }

      if (prop === "pruneExpired") {
        return pruneExpired;
      }

      return Reflect.get(target, prop, receiver);
    }
  });

  return proxy;
}

const jobs = createPrunableJobsMap();
const jobsCleanupTimer = setInterval(() => {
  jobs.pruneExpired();
}, JOB_CLEANUP_INTERVAL_MS);

if (typeof jobsCleanupTimer.unref === "function") {
  jobsCleanupTimer.unref();
}

const storage = multer.diskStorage({
  destination: async (_req, _file, cb) => {
    try {
      await fs.mkdir(uploadDir, { recursive: true });
      cb(null, uploadDir);
    } catch (error) {
      cb(error);
    }
  },
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname || "") || ".mp4";
    cb(null, `${Date.now()}-${crypto.randomUUID()}${ext}`);
  }
});

const upload = multer({ storage });

function requireGatewayToken(req, res, next) {
  if (!GATEWAY_TOKEN) return next();

  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : req.headers["x-forge-token"];

  if (token !== GATEWAY_TOKEN) {
    return res.status(401).json({
      ok: false,
      error: "Unauthorized",
      message: "Missing or invalid Forge gateway token."
    });
  }

  next();
}

function nowIso() {
  return new Date().toISOString();
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
  return JSON.parse(normalized);
}

async function runGeminiVideoAnalysis(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  job.status = "processing";
  job.processing_started_at = nowIso();
  jobs.set(jobId, job);
  console.log(`[video-job] processing job_id=${jobId}`);

  try {
    if (!GEMINI_API_KEY) {
      throw new Error("GEMINI_API_KEY is not configured.");
    }

    const fileBuffer = await fs.readFile(job.filepath);
    const videoBase64 = fileBuffer.toString("base64");

    const prompt = `Analyze this episode video and return ONLY valid JSON with exactly these top-level keys:\n\nstory_summary (string)\nscene_list (array)\ncharacters (array)\nlocations (array)\nvisual_style (string)\nemotional_beats (array)\nunresolved_hooks (array)\nnext_episode_outline (string)\nnext_scene_prompts (array)\n\nBe concise but complete. No markdown, no code fences.`;

    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;

    const geminiResponse = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              { text: prompt },
              {
                inline_data: {
                  mime_type: job.mimetype || "video/mp4",
                  data: videoBase64
                }
              }
            ]
          }
        ],
        generationConfig: {
          temperature: 0.3,
          responseMimeType: "application/json"
        }
      })
    });

    const payload = await geminiResponse.json();

    if (!geminiResponse.ok) {
      throw new Error(`Gemini API HTTP ${geminiResponse.status}: ${JSON.stringify(payload)}`);
    }

    const text = payload?.candidates?.[0]?.content?.parts?.map((p) => p.text || "").join("\n") || "";
    if (!text) {
      throw new Error("Gemini returned empty content.");
    }

    const parsed = safeParseGeminiJson(text);
    const result = {
      ...createEmptyAnalysis(),
      ...parsed
    };

    job.status = "analysis_complete";
    job.completed_at = nowIso();
    job.result = result;
    jobs.set(jobId, job);

    console.log(`[video-job] complete job_id=${jobId}`);
  } catch (error) {
    job.status = "analysis_failed";
    job.completed_at = nowIso();
    job.error = error instanceof Error ? error.message : String(error);
    jobs.set(jobId, job);
    console.error(`[video-job] failed job_id=${jobId}`, error);
  } finally {
    if (job.filepath) {
      try {
        await fs.unlink(job.filepath);
      } catch (cleanupError) {
        if (!(cleanupError instanceof Error) || cleanupError.code !== "ENOENT") {
          console.error(`[video-job] cleanup failed job_id=${jobId} filepath=${job.filepath}`, cleanupError);
        }
      }
    }
  }
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "SAIN Forge Railway Gateway",
    version: "v2-async-video-jobs",
    status: "online",
    routes: ["/health", "/providers/test", "/video/read", "/video/result/:job_id", "/story/continue"],
    time: nowIso()
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "forge-gateway",
    status: "online",
    time: nowIso()
  });
});

app.post("/providers/test", requireGatewayToken, (req, res) => {
  res.json({
    ok: true,
    provider_keys_visible_to_local_forge: false,
    railway_env_present: {
      GEMINI_API_KEY: Boolean(process.env.GEMINI_API_KEY),
      FORGE_GATEWAY_TOKEN: Boolean(process.env.FORGE_GATEWAY_TOKEN),
      GEMINI_MODEL: process.env.GEMINI_MODEL || "gemini-2.5-flash"
    },
    message: "Gateway is online. Async Gemini video reader is enabled."
  });
});

app.post("/video/read", requireGatewayToken, upload.single("video"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      ok: false,
      error: "Missing video input",
      message: "Send multipart/form-data with field named video."
    });
  }

  const jobId = `forge_video_${Date.now()}_${crypto.randomUUID()}`;
  jobs.set(jobId, {
    job_id: jobId,
    status: "queued",
    created_at: nowIso(),
    filepath: req.file.path,
    mimetype: req.file.mimetype,
    originalname: req.file.originalname,
    size_bytes: req.file.size,
    result: null,
    error: null
  });

  console.log(`[video-job] queued job_id=${jobId} file=${req.file.path} size=${req.file.size}`);

  res.json({
    ok: true,
    job_id: jobId,
    status: "queued",
    uploaded_file: {
      originalname: req.file.originalname,
      mimetype: req.file.mimetype,
      size_bytes: req.file.size
    },
    result_url: `/video/result/${jobId}`
  });

  setImmediate(() => {
    runGeminiVideoAnalysis(jobId);
  });
});

app.get("/video/result/:job_id", requireGatewayToken, (req, res) => {
  const { job_id: jobId } = req.params;
  const job = jobs.get(jobId);

  if (!job) {
    return res.status(404).json({ ok: false, error: "Job not found", job_id: jobId });
  }

  if (job.status === "queued" || job.status === "processing") {
    return res.json({
      ok: true,
      job_id: jobId,
      status: job.status,
      created_at: job.created_at,
      processing_started_at: job.processing_started_at || null
    });
  }

  if (job.status === "analysis_failed") {
    return res.json({
      ok: true,
      job_id: jobId,
      status: "analysis_failed",
      error: job.error,
      completed_at: job.completed_at
    });
  }

  return res.json({
    ok: true,
    job_id: jobId,
    status: "analysis_complete",
    completed_at: job.completed_at,
    ...job.result
  });
});

app.post("/story/continue", requireGatewayToken, (req, res) => {
  const body = req.body || {};

  res.json({
    ok: true,
    status: "stub_created",
    created_at: nowIso(),
    input_summary: {
      has_story_summary: Boolean(body.story_summary),
      scene_count: Array.isArray(body.scene_list) ? body.scene_list.length : 0,
      character_count: Array.isArray(body.characters) ? body.characters.length : 0
    },
    next_episode_plan: {
      title: "Next AzoZeo Episode Draft",
      logline: "AzoZeo follows the unresolved signal into the next stage of his awakening.",
      scenes: [
        {
          beat: "Opening continuation",
          purpose: "Reconnect visually and emotionally to the previous episode ending."
        },
        {
          beat: "Character choice",
          purpose: "AzoZeo makes a decision that pushes the story forward."
        },
        {
          beat: "Mystery escalation",
          purpose: "Reveal a new symbol, mentor signal, or hidden-world clue."
        }
      ]
    }
  });
});

app.listen(PORT, () => {
  console.log(`SAIN Forge Railway Gateway online on port ${PORT}`);
});

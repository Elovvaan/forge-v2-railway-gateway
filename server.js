import express from "express";
import cors from "cors";
import multer from "multer";

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(express.json({ limit: "25mb" }));

const PORT = process.env.PORT || 3000;
const GATEWAY_TOKEN = process.env.FORGE_GATEWAY_TOKEN || "";

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

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "SAIN Forge Railway Gateway",
    version: "v1",
    status: "online",
    routes: ["/health", "/providers/test", "/video/read", "/story/continue"],
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
      OPENAI_API_KEY: Boolean(process.env.OPENAI_API_KEY),
      GEMINI_API_KEY: Boolean(process.env.GEMINI_API_KEY),
      ELEVENLABS_API_KEY: Boolean(process.env.ELEVENLABS_API_KEY),
      RUNWAY_API_KEY: Boolean(process.env.RUNWAY_API_KEY),
      KLING_API_KEY: Boolean(process.env.KLING_API_KEY),
      LUMA_API_KEY: Boolean(process.env.LUMA_API_KEY)
    },
    message: "Gateway is online. Provider adapters can be added next."
  });
});

app.post("/video/read", requireGatewayToken, upload.single("video"), async (req, res) => {
  const body = req.body || {};
  const videoUrl = body.video_url || body.videoUrl || null;
  const hasUploadedVideo = Boolean(req.file);

  if (!videoUrl && !hasUploadedVideo) {
    return res.status(400).json({
      ok: false,
      error: "Missing video input",
      message: "Send JSON { video_url } or multipart upload field named video."
    });
  }

  const jobId = `forge_video_${Date.now()}`;

  const result = {
    ok: true,
    job_id: jobId,
    status: "stub_received",
    received_at: nowIso(),
    input: {
      video_url: videoUrl,
      uploaded_file: hasUploadedVideo
        ? {
            originalname: req.file.originalname,
            mimetype: req.file.mimetype,
            size_bytes: req.file.size
          }
        : null
    },
    story_summary: "Video received by Forge Gateway. Real Gemini/OpenAI video analysis adapter is the next layer.",
    scene_list: [],
    characters: [],
    emotional_beats: [],
    unresolved_hooks: [],
    next_scene_prompts: [
      {
        scene_number: 1,
        prompt: "Continue the story using the uploaded episode as continuity reference."
      }
    ]
  };

  res.json(result);
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

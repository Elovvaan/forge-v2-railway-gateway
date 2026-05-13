# SAIN Forge Railway Gateway

Cloud API bridge for SAIN / Forge.

## Routes

- `GET /health`
- `POST /providers/test`
- `POST /video/read`
- `POST /story/continue`

## Railway variables

Add these inside Railway > Service > Variables:

```env
FORGE_GATEWAY_TOKEN=your-private-token
OPENAI_API_KEY=
GEMINI_API_KEY=
ELEVENLABS_API_KEY=
RUNWAY_API_KEY=
KLING_API_KEY=
LUMA_API_KEY=
```

## Local test

```bash
npm install
npm start
```

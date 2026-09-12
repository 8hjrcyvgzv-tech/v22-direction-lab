V2.2.1 startup-diagnostic patch

Purpose:
- Keeps research-only behavior (no orders, no paper, no ROI/TP/SL).
- Does not touch V2.1.10.
- Catches /start startup failures and returns the real stage/message as JSON instead of Cloudflare 1101.
- Adds startStage and structured lastError to /health.
- Uses fresh v221:* state keys so V2.2.0 partial startup state cannot contaminate this run.

Upload these two files to the existing v22-direction-lab GitHub repo:
1) v22_direction_lab_v2_2_1.js
2) wrangler.toml (replace existing wrangler.toml)

Cloudflare Git integration will auto-deploy the commit.
Then open /start once, followed by /health.
If startup still fails, /start itself will now show startupError.stage and startupError.message.

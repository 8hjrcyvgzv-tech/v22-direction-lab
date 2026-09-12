V2.2.2 Direction Lab — BingX-native startup/data patch

Reason:
- V2.2.1 startup diagnostic proved Binance fapi exchangeInfo returns HTTP 403
  from the Cloudflare Worker environment.
- This is an upstream access restriction, not a Direction Lab model error.

Changes:
- Removes Binance REST and Binance WebSocket dependencies.
- Research universe comes only from BingX public swap contracts.
- Short-window trade flow comes from BingX public real-time {symbol}@trade WebSocket.
- Handles BingX GZIP frames and Ping/Pong heartbeat.
- Splits universe across conservative 100-subscription WebSocket chunks.
- Keeps marketEventAt and detectionAt separate.
- Keeps raw direction labels separate from raw return metrics.
- Keeps 10s/30s/1m/3m/5m/15m forward labels.
- Keeps V2.1.10 untouched.
- Uses fresh v222:* state keys and Durable Object name v222:lab.

Upload both files to the existing v22-direction-lab GitHub repo:
1) v22_direction_lab_v2_2_2.js
2) wrangler.toml (replace current wrangler.toml with this file's contents/name)

Cloudflare Git integration should deploy automatically.
After deployment:
- /health should show DIRECTION_LAB_V2.2.2
- open /start once
- then check /health until running=true and WebSocket status OPEN (or OPEN_n_OF_n)

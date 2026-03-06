#!/bin/sh
# WPPConnect Server Entrypoint
# Patches the compiled config to inject WEBHOOK_URL from environment
# before starting the server.
# WEBHOOK_URL must be set as an env var on the Railway service (e.g. cozy-rejoicing)

CONFIG_FILE="dist/config.js"

if [ -n "$WEBHOOK_URL" ]; then
  echo "[entrypoint] Injecting WEBHOOK_URL: $WEBHOOK_URL"
  # Replace url: null with the actual URL in the compiled config
  sed -i "s|url: null|url: '${WEBHOOK_URL}'|g" "$CONFIG_FILE"
  echo "[entrypoint] Config patched successfully"
else
  echo "[entrypoint] WARNING: WEBHOOK_URL not set, webhooks will NOT be sent"
fi

# Show the webhook config for verification
echo "[entrypoint] Webhook config:"
grep -A2 "webhook:" "$CONFIG_FILE" | head -5

echo "[entrypoint] Starting WPPConnect Server..."
exec node dist/server.js

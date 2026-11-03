#!/bin/sh
# Install the claude-bridge plugin into a Hermes home (HERMES_HOME, default ~/.hermes).
# Changes that home's plugins directory and plugins.enabled. Restart Hermes afterwards.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
home=${HERMES_HOME:-$HOME/.hermes}
mkdir -p "$home/plugins"
rm -rf "$home/plugins/claude-bridge"
cp -r "$here/hermes_plugin/claude-bridge" "$home/plugins/claude-bridge"
rm -rf "$home/plugins/claude-bridge/__pycache__"
hermes plugins enable claude-bridge </dev/null
cat <<EOF

Installed into $home/plugins/claude-bridge. Still to do by hand:

1. Add to $home/.env:
     CLAUDE_BRIDGE_URL=http://127.0.0.1:8765
     CLAUDE_BRIDGE_TOKEN=<same as BRIDGE_API_TOKEN>
     CLAUDE_BRIDGE_ADAPTER_URL=http://127.0.0.1:8766
     CLAUDE_BRIDGE_ADAPTER_TOKEN=<same as BRIDGE_ADAPTER_TOKEN>

2. For wake-ups in messaging conversations, add to $home/config.yaml:
     plugins:
       entries:
         claude-bridge:
           allow_gateway_injection: true

3. Check that the claude_bridge toolset is on for your platform: hermes tools list

4. Restart the gateway: hermes gateway restart
EOF

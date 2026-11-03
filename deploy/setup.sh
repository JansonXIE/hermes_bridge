#!/usr/bin/env bash
# One-step install of the bridge on this machine, as the current user. Safe to re-run: existing
# secrets and configuration are kept, and new --repo paths are added to the existing projects.
#
#   deploy/setup.sh --repo /path/to/git-repo --repo /path/to/repo-workspace [options]
#
# Steps: check prerequisites, create the bridge home (bridge.toml, bridge.env, data/), add projects,
# install and start the systemd user services, then install the claude-bridge plugin into Hermes.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: deploy/setup.sh [options]

  --home DIR          bridge home with bridge.toml, bridge.env and data/ (default: ~/hermes-bridge)
  --repo PATH[=ID]    add a git repository or a `repo` workspace; repeatable. ID names the project
                      (git repository) or prefixes the ids (repo workspace); default: directory name
  --full-access       git projects run with bypassPermissions: any tool and shell command, including
                      builds, git fetch and git push. No sandbox. Default: read and edit code only
  --api-port N        default 8765 (only used when bridge.toml is created)
  --adapter-port N    default 8766 (only used when bridge.toml is created)
  --service NAME      systemd unit name (default: hermes-bridge); use another for a second bridge
  --no-systemd        do not install or start services
  --no-hermes         do not touch Hermes
  --restart-gateway   run `hermes gateway restart` at the end (interrupts messaging briefly)
  -h, --help
EOF
}

BRIDGE_HOME="$HOME/hermes-bridge"; SERVICE=hermes-bridge; API_PORT=8765; ADAPTER_PORT=8766
REPOS=(); FULL=(); SYSTEMD=1; HERMES=1; RESTART_GATEWAY=0
while [ $# -gt 0 ]; do
    case "$1" in
        --home) BRIDGE_HOME=$2; shift ;;
        --repo) REPOS+=("$2"); shift ;;
        --full-access) FULL=(--full-access) ;;
        --api-port) API_PORT=$2; shift ;;
        --adapter-port) ADAPTER_PORT=$2; shift ;;
        --service) SERVICE=$2; shift ;;
        --no-systemd) SYSTEMD=0 ;;
        --no-hermes) HERMES=0 ;;
        --restart-gateway) RESTART_GATEWAY=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

say() { printf '\n==> %s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

CODE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BRIDGE_HOME=$(realpath -m -- "$BRIDGE_HOME")
CONFIG="$BRIDGE_HOME/bridge.toml"
UNIT="$HOME/.config/systemd/user/$SERVICE@.service"
# Checked before anything is created: re-pointing a running bridge at an empty home would orphan its tasks.
if [ "$SYSTEMD" = 1 ] && [ -f "$UNIT" ] && ! grep -q -- "-c $BRIDGE_HOME/bridge.toml" "$UNIT"; then
    die "$UNIT already serves another bridge home ($(grep -o -- '-c [^ ]*' "$UNIT")). Use --home with that directory, or --service with another name"
fi

say "Checking prerequisites"
PYTHON=
for candidate in python3.13 python3.12 python3.11 python3; do
    if path=$(command -v "$candidate") && "$path" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
        PYTHON=$path; break
    fi
done
[ -n "$PYTHON" ] || die "Python 3.11 or newer is required (tomllib)"
command -v git >/dev/null || die "git is required"
CLAUDE=$(command -v claude) || die "the claude CLI is required: https://docs.claude.com/en/docs/claude-code"
echo "python: $PYTHON ($("$PYTHON" -V 2>&1)), claude: $CLAUDE ($("$CLAUDE" --version 2>/dev/null | head -1))"
bridge() { PYTHONPATH="$CODE_DIR" "$PYTHON" -m hermes_bridge -c "$CONFIG" "$@"; }

say "Bridge home $BRIDGE_HOME"
mkdir -p "$BRIDGE_HOME"
chmod 700 "$BRIDGE_HOME"
bridge init --api-port "$API_PORT" --adapter-port "$ADAPTER_PORT" --claude "$CLAUDE"

for spec in ${REPOS[@]+"${REPOS[@]}"}; do
    path=${spec%%=*}; name=
    [ "$spec" != "$path" ] && name=${spec#*=}
    say "Adding $path"
    bridge add-projects ${FULL[@]+"${FULL[@]}"} ${name:+--name "$name"} "$path"
done
bridge check

"$PYTHON" - "$BRIDGE_HOME/bridge.env" <<'EOF' || warn "no Claude credentials found. Under systemd, log in with \`claude\` once, or add ANTHROPIC_* lines to bridge.env"
import json, os, sys
from pathlib import Path
claude_dir = Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home() / ".claude"))
env_file = Path(sys.argv[1]).read_text()
try:
    settings_env = json.loads((claude_dir / "settings.json").read_text()).get("env", {})
except (OSError, ValueError):
    settings_env = {}
ok = ((claude_dir / ".credentials.json").exists() or any(k.startswith("ANTHROPIC_") for k in settings_env)
      or "ANTHROPIC_" in env_file)
sys.exit(0 if ok else 1)
EOF

read_setting() {  # api.port / adapter.port / tokens, as the services will see them
    PYTHONPATH="$CODE_DIR" "$PYTHON" -c "
import sys; from pathlib import Path
from hermes_bridge.provision import load_env_file; from hermes_bridge.config import load_config
load_env_file(Path(sys.argv[1]).parent / 'bridge.env'); c = load_config(sys.argv[1])
print({'api_port': c.api_port, 'adapter_port': c.adapter_port, 'api_token': c.principals[0].token,
       'adapter_token': c.adapter_token}[sys.argv[2]])" "$CONFIG" "$1"
}

api_health() {
    "$PYTHON" - "$1" "$2" <<'EOF'
import sys, urllib.request
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
req = urllib.request.Request(f"http://127.0.0.1:{sys.argv[1]}/v1/health", headers={"Authorization": f"Bearer {sys.argv[2]}"})
print(opener.open(req, timeout=2).read().decode())
EOF
}

if [ "$SYSTEMD" = 1 ]; then
    say "systemd user services $SERVICE@{api,worker,adapter}"
    command -v systemctl >/dev/null || die "systemctl not found; re-run with --no-systemd and start the roles yourself"
    unit=$UNIT
    mkdir -p "$(dirname "$unit")"
    rendered=$(sed -e "s|@CODE_DIR@|$CODE_DIR|g" -e "s|@BRIDGE_HOME@|$BRIDGE_HOME|g" -e "s|@PYTHON@|$PYTHON|g" \
        "$CODE_DIR/deploy/hermes-bridge@.service")
    if [ ! -f "$unit" ] || [ "$(cat "$unit")" != "$rendered" ]; then
        [ -f "$unit" ] && cp -p "$unit" "$unit.bak.$(date +%Y%m%d_%H%M%S)"
        printf '%s\n' "$rendered" > "$unit"
    fi
    systemctl --user daemon-reload
    systemctl --user enable "$SERVICE@api" "$SERVICE@worker" "$SERVICE@adapter" >/dev/null 2>&1
    # A worker restart does not stop running Claude processes: the next worker adopts them.
    systemctl --user restart "$SERVICE@api" "$SERVICE@worker" "$SERVICE@adapter"
    if [ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null)" != yes ]; then
        loginctl enable-linger "$USER" 2>/dev/null \
            || warn "services stop when you log out. Ask an admin to run: sudo loginctl enable-linger $USER"
    fi

    api_port=$(read_setting api_port); api_token=$(read_setting api_token)
    for _ in $(seq 30); do
        health=$(api_health "$api_port" "$api_token" 2>/dev/null) && break
        sleep 1
    done
    [ -n "${health:-}" ] || die "the API did not answer on port $api_port. See: journalctl --user -u '$SERVICE@*' -n 50"
    echo "health: $health"
fi

if [ "$HERMES" = 1 ]; then
    if ! command -v hermes >/dev/null; then
        warn "hermes not found; skipped the Hermes plugin. Re-run after installing Hermes Agent"
    else
        hermes_config=$(hermes config path </dev/null 2>/dev/null | tail -1)
        hermes_env=$(hermes config env-path </dev/null 2>/dev/null | tail -1)
        hermes_home=$(dirname "$hermes_config")
        say "Hermes plugin into $hermes_home"
        mkdir -p "$hermes_home/plugins"
        rm -rf "$hermes_home/plugins/claude-bridge"
        cp -r "$CODE_DIR/hermes_plugin/claude-bridge" "$hermes_home/plugins/claude-bridge"
        rm -rf "$hermes_home/plugins/claude-bridge/__pycache__"
        hermes plugins enable claude-bridge --no-allow-tool-override </dev/null >/dev/null

        [ -f "$hermes_env" ] && cp -p "$hermes_env" "$hermes_env.bak.$(date +%Y%m%d_%H%M%S)"
        PYTHONPATH="$CODE_DIR" "$PYTHON" - "$hermes_env" \
            "http://127.0.0.1:$(read_setting api_port)" "$(read_setting api_token)" \
            "http://127.0.0.1:$(read_setting adapter_port)" "$(read_setting adapter_token)" <<'EOF'
import sys
from pathlib import Path
from hermes_bridge.provision import set_env_vars
keys = ("CLAUDE_BRIDGE_URL", "CLAUDE_BRIDGE_TOKEN", "CLAUDE_BRIDGE_ADAPTER_URL", "CLAUDE_BRIDGE_ADAPTER_TOKEN")
changed = set_env_vars(Path(sys.argv[1]), dict(zip(keys, sys.argv[2:])))
print(f"{sys.argv[1]}: " + (f"set {', '.join(changed)}" if changed else "already up to date"))
EOF

        key=plugins.entries.claude-bridge.allow_gateway_injection
        if [ "$(hermes config get "$key" </dev/null 2>/dev/null | tail -1)" != True ]; then
            # `hermes config set` rewrites config.yaml without its comments: keep a copy.
            [ -f "$hermes_config" ] && cp -p "$hermes_config" "$hermes_config.bak.$(date +%Y%m%d_%H%M%S)-pre-claude-bridge"
            hermes config set "$key" true </dev/null | tail -1
        fi
        hermes tools list </dev/null 2>/dev/null | grep -i claude_bridge || warn "claude_bridge toolset not listed by: hermes tools list"

        if [ "$RESTART_GATEWAY" = 1 ]; then
            say "Restarting the Hermes gateway"
            hermes gateway restart </dev/null | tail -2
        else
            echo "Restart the Hermes gateway (and open Hermes CLI sessions) to load the plugin: hermes gateway restart"
        fi
    fi
fi

say "Done"
cat <<EOF
Add projects:  edit $CONFIG (commented examples at the end), then
               PYTHONPATH=$CODE_DIR $PYTHON -m hermes_bridge -c $CONFIG check
               systemctl --user restart $SERVICE@api $SERVICE@worker
List projects: PYTHONPATH=$CODE_DIR $PYTHON -m hermes_bridge -c $CONFIG projects
Logs:          journalctl --user -u '$SERVICE@*' -f
In Hermes, name the project id when asking for a Claude Code task.
EOF

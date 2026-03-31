#!/usr/bin/env bash
# disk_guard.sh — Monitor a directory and stop a tmux pipeline when size limit is reached.
#                 Optionally sends periodic Telegram notifications.
#
# Usage:
#   ./scripts/disk_guard.sh [OPTIONS]
#
# Options:
#   -d DIR          Directory to monitor (default: downloads/harvard-dataverse)
#   -l LIMIT        Size limit, e.g. 500G, 1T (default: 600G)
#   -s SESSION      tmux session name (default: harvard-dataverse)
#   -i INTERVAL     Check interval in seconds (default: 300 = 5 min)
#   -n NOTIFY       Telegram notify interval in seconds (default: 3600 = 1 hr)
#   -b DB           SQLite database path (default: metadata/qdarchive.sqlite)
#   -r REPO_URL     Repository URL filter for stats (default: https://dataverse.harvard.edu)
#   -h              Show help
#
# Telegram setup:
#   Set these env vars (or add to .env):
#     TELEGRAM_BOT_TOKEN=<your bot token>
#     TELEGRAM_CHAT_ID=<your chat id>

set -euo pipefail

DIR="downloads/harvard-dataverse"
LIMIT="600G"
SESSION="harvard-dataverse"
INTERVAL=300
NOTIFY_INTERVAL=3600
DB="metadata/qdarchive.sqlite"
REPO_URL="https://dataverse.harvard.edu"

usage() {
    sed -n '2,22p' "$0" | sed 's/^# \?//'
    exit 0
}

while getopts "d:l:s:i:n:b:r:h" opt; do
    case $opt in
        d) DIR="$OPTARG" ;;
        l) LIMIT="$OPTARG" ;;
        s) SESSION="$OPTARG" ;;
        i) INTERVAL="$OPTARG" ;;
        n) NOTIFY_INTERVAL="$OPTARG" ;;
        b) DB="$OPTARG" ;;
        r) REPO_URL="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Load .env if present
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"
TELEGRAM_ENABLED=false
if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
    TELEGRAM_ENABLED=true
fi

parse_size() {
    local val="${1%[A-Za-z]*}"
    local unit="${1##*[0-9.]}"
    case "${unit^^}" in
        T|TB) echo "$(awk "BEGIN {printf \"%.0f\", $val * 1024 * 1024 * 1024 * 1024}")" ;;
        G|GB) echo "$(awk "BEGIN {printf \"%.0f\", $val * 1024 * 1024 * 1024}")" ;;
        M|MB) echo "$(awk "BEGIN {printf \"%.0f\", $val * 1024 * 1024}")" ;;
        *)    echo "$val" ;;
    esac
}

human_size() {
    local bytes=$1
    if (( bytes >= 1099511627776 )); then
        awk "BEGIN {printf \"%.1f TB\", $bytes / 1099511627776}"
    elif (( bytes >= 1073741824 )); then
        awk "BEGIN {printf \"%.1f GB\", $bytes / 1073741824}"
    else
        awk "BEGIN {printf \"%.1f MB\", $bytes / 1048576}"
    fi
}

send_telegram() {
    local message="$1"
    if [ "$TELEGRAM_ENABLED" = true ]; then
        curl -s -X POST \
            "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="$TELEGRAM_CHAT_ID" \
            -d parse_mode="Markdown" \
            -d text="$message" > /dev/null 2>&1 || true
    fi
}

get_pipeline_stats() {
    if [ ! -f "$DB" ]; then
        echo "DB not found"
        return
    fi
    sqlite3 "$DB" "
    SELECT
      SUM(CASE WHEN total_files = 0 THEN 1 ELSE 0 END),
      SUM(CASE WHEN total_files > 0 AND success = total_files THEN 1 ELSE 0 END),
      SUM(CASE WHEN success > 0 AND success < total_files THEN 1 ELSE 0 END),
      SUM(CASE WHEN total_files > 0 AND success = 0 AND skipped = total_files THEN 1 ELSE 0 END),
      SUM(CASE WHEN total_files > 0 AND success = 0 AND unknown = total_files THEN 1 ELSE 0 END),
      COUNT(*),
      SUM(success),
      SUM(total_files)
    FROM (
      SELECT p.id,
        COUNT(f.id) as total_files,
        SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
        SUM(CASE WHEN f.status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped,
        SUM(CASE WHEN f.status = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown
      FROM projects p
      LEFT JOIN files f ON f.project_id = p.id
      WHERE p.repository_url = '$REPO_URL'
      GROUP BY p.id
    );
    "
}

build_status_message() {
    local current_human="$1"
    local limit_human="$2"
    local remaining_human="$3"
    local alert="$4"  # "" or "LIMIT EXCEEDED"

    local header
    if [ -n "$alert" ]; then
        header="🚨 *DISK GUARD: LIMIT EXCEEDED*"
    else
        header="📊 *Disk Guard Status*"
    fi

    local msg="${header}
⏰ $(date '+%Y-%m-%d %H:%M:%S')

💾 *Disk Usage*
  Used: ${current_human} / ${limit_human}
  Remaining: ${remaining_human}"

    # Pipeline stats
    local stats
    stats=$(get_pipeline_stats)
    if [ -n "$stats" ] && [ "$stats" != "DB not found" ]; then
        IFS='|' read -r no_files fully partial_dl all_skipped not_started total_ds total_success total_files <<< "$stats"
        msg="${msg}

📦 *Pipeline ($(basename "$REPO_URL"))*
  Total datasets: ${total_ds}
  Fully downloaded: ${fully}
  Partially downloaded: ${partial_dl}
  All access-denied: ${all_skipped}
  Not started: ${not_started}
  No file records: ${no_files}
  Files: ${total_success} / ${total_files} downloaded"
    fi

    # tmux session status
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        msg="${msg}

✅ tmux session \`${SESSION}\` is running"
    else
        msg="${msg}

❌ tmux session \`${SESSION}\` not found"
    fi

    echo "$msg"
}

LIMIT_BYTES=$(parse_size "$LIMIT")

echo "=== Disk Guard ==="
echo "  Directory : $DIR"
echo "  Limit     : $LIMIT ($(human_size "$LIMIT_BYTES"))"
echo "  Session   : $SESSION"
echo "  Interval  : ${INTERVAL}s"
echo "  Notify    : ${NOTIFY_INTERVAL}s"
echo "  Telegram  : $TELEGRAM_ENABLED"
echo "  DB        : $DB"
echo "  Repo      : $REPO_URL"
echo ""

if [ ! -d "$DIR" ]; then
    echo "ERROR: Directory '$DIR' does not exist."
    exit 1
fi

# Send startup notification
STARTUP_MSG="🟢 *Disk Guard Started*
Monitoring: \`$DIR\`
Limit: $(human_size "$LIMIT_BYTES")
Session: \`$SESSION\`
Notify every: $((NOTIFY_INTERVAL / 60)) min"
send_telegram "$STARTUP_MSG"

LAST_NOTIFY=0

while true; do
    CURRENT_BYTES=$(du -sb "$DIR" 2>/dev/null | cut -f1)
    CURRENT_HUMAN=$(human_size "$CURRENT_BYTES")
    LIMIT_HUMAN=$(human_size "$LIMIT_BYTES")
    NOW=$(date '+%Y-%m-%d %H:%M:%S')
    NOW_EPOCH=$(date +%s)

    if (( CURRENT_BYTES >= LIMIT_BYTES )); then
        echo "[$NOW] LIMIT EXCEEDED: $CURRENT_HUMAN >= $LIMIT_HUMAN"
        echo "[$NOW] Sending interrupt to tmux session '$SESSION'..."

        # Send Telegram alert
        MSG=$(build_status_message "$CURRENT_HUMAN" "$LIMIT_HUMAN" "0 B" "LIMIT EXCEEDED")
        send_telegram "$MSG"

        if tmux has-session -t "$SESSION" 2>/dev/null; then
            tmux send-keys -t "$SESSION" C-c
            sleep 2
            tmux send-keys -t "$SESSION" C-c
            echo "[$NOW] Interrupt sent. Pipeline should stop shortly."
            send_telegram "🛑 Interrupt sent to \`$SESSION\`. Pipeline stopping."
        else
            echo "[$NOW] WARNING: tmux session '$SESSION' not found!"
        fi
        exit 0
    fi

    REMAINING_BYTES=$((LIMIT_BYTES - CURRENT_BYTES))
    REMAINING_HUMAN=$(human_size "$REMAINING_BYTES")
    echo "[$NOW] $CURRENT_HUMAN / $LIMIT_HUMAN used ($REMAINING_HUMAN remaining)"

    # Periodic Telegram notification
    if (( NOW_EPOCH - LAST_NOTIFY >= NOTIFY_INTERVAL )); then
        MSG=$(build_status_message "$CURRENT_HUMAN" "$LIMIT_HUMAN" "$REMAINING_HUMAN" "")
        send_telegram "$MSG"
        LAST_NOTIFY=$NOW_EPOCH
    fi

    sleep "$INTERVAL"
done

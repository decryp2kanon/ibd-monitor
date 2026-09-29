#!/bin/bash

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
mapfile -d '' -t CONNECTION < <(python3 -c '
import os, sys
sys.path.insert(0, sys.argv[1])
from ibd_connection import configuration
datadir, cli, options, debug_log = configuration(sys.argv[1])
for value in (str(datadir), cli, str(debug_log), *options):
    sys.stdout.buffer.write(os.fsencode(value) + b"\0")
' "$SCRIPT_DIR")
if (( ${#CONNECTION[@]} < 3 )); then
    echo 'Unable to resolve node configuration' >&2
    exit 1
fi
DATADIR=${CONNECTION[0]}
CLI=${CONNECTION[1]}
DEBUG_LOG=${CONNECTION[2]}
RPC_OPTIONS=("${CONNECTION[@]:3}")

LOGDIR="${IBD_LOGDIR:-$SCRIPT_DIR}"
i=1
while [ -f "$LOGDIR/ibd_test_$i.txt" ]
do
    i=$((i+1))
done

LOGFILE="$LOGDIR/ibd_test_$i.txt"

last_height=0
last_headers=0
last_sample_time=0
last_header_stage=""

# Read this datadir's log incrementally. An already-running graph collector may
# use an older parser; terminal progress must not depend on its cached CSV rows.
coproc PROGRESS_READER {
    python3 -u -c '
import json, sys
sys.path.insert(0, sys.argv[1])
from ibd_terminal_progress import TerminalLogReader
reader = TerminalLogReader(sys.argv[2])
try:
    for request in sys.stdin:
        print(json.dumps(reader.poll()), flush=True)
except KeyboardInterrupt:
    pass
' "$SCRIPT_DIR" "$DEBUG_LOG"
}
PROGRESS_PID=$PROGRESS_READER_PID
PROGRESS_INPUT=${PROGRESS_READER[1]}
PROGRESS_OUTPUT=${PROGRESS_READER[0]}
trap 'exec {PROGRESS_INPUT}>&-; wait "$PROGRESS_PID" 2>/dev/null' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

while true
do
    CHAININFO=$("$CLI" \
      -datadir="$DATADIR" \
      "${RPC_OPTIONS[@]}" \
      getblockchaininfo 2>/dev/null)

    B=$(jq -r '.blocks // empty' <<< "$CHAININFO" 2>/dev/null)
    H=$(jq -r '.headers // empty' <<< "$CHAININFO" 2>/dev/null)

    if ! [[ "$B" =~ ^[0-9]+$ && "$H" =~ ^[0-9]+$ ]]; then
        echo "[$(date '+%H:%M:%S')]\tRPC 연결 대기 중..." | tee -a "$LOGFILE"
        sleep 5
        continue
    fi

    PEERINFO=$("$CLI" \
      -datadir="$DATADIR" \
      "${RPC_OPTIONS[@]}" \
      getpeerinfo 2>/dev/null)

    P=$(jq -r 'if type == "array" then [.[] | select(.inbound == false)] | length else "n/a" end' <<< "$PEERINFO" 2>/dev/null) || P="n/a"

    P=${P:-n/a}

    now=$(date +%s)
    if [ "$last_sample_time" -eq 0 ]; then
        sample_seconds=0
    else
        sample_seconds=$((now-last_sample_time))
    fi


    if [ "$last_sample_time" -eq 0 ]; then
        diff=0
    else
        diff=$((B-last_height))
    fi


    if [ "$sample_seconds" -gt 0 ]; then
        S=$((diff/sample_seconds))
    else
        S=0
    fi

    if [ "$last_sample_time" -eq 0 ]; then
        header_diff=0
    else
        header_diff=$((H-last_headers))
    fi

    if [ "$sample_seconds" -gt 0 ]; then
        HS=$((header_diff/sample_seconds))
    else
        HS=0
    fi


    if ! printf 'poll\n' >&"$PROGRESS_INPUT" || ! IFS= read -r CHECKPOINT <&"$PROGRESS_OUTPUT"; then
        CHECKPOINT='{"checkpoint_phase":"unavailable"}'
    fi
    if ! jq -e 'type == "object"' >/dev/null 2>&1 <<< "$CHECKPOINT"; then
        CHECKPOINT='{"checkpoint_phase":"unavailable"}'
    fi
    CP=$(jq -r '.checkpoint_phase | if . == null or . == "" then "unavailable" else . end' <<< "$CHECKPOINT")
    header_stage=$CP
    DISPLAY_H=$H
    if [[ "$CP" == "presync" || "$CP" == "replay" ]]; then
        DISPLAY_H=$(jq -r --arg key "${CP}_height" '.[$key] | if . == null or . == "" then "n/a" else . end' <<< "$CHECKPOINT")
        HS=$(jq -r --arg key "${CP}_rate" '.[$key] | if . == null or . == "" then "n/a" else . end' <<< "$CHECKPOINT")
    elif [[ "$CP" == "core31" || "$CP" == "core31_complete" ]]; then
        DISPLAY_H=$(jq -r '.core31_height | if . == null or . == "" then "n/a" else . end' <<< "$CHECKPOINT")
        if [[ "$CP" == "core31_complete" && "$DISPLAY_H" =~ ^[0-9]+$ ]] && (( H > DISPLAY_H )); then
            DISPLAY_H=$H
        else
            HS=$(jq -r '.core31_rate | if . == null or . == "" then "n/a" else . end' <<< "$CHECKPOINT")
        fi
    else
        header_stage="headers"
        if [[ "$last_header_stage" != "$header_stage" ]] || (( header_diff < 0 )); then
            HS="n/a"
        fi
    fi
    if (( last_sample_time == 0 || diff < 0 )); then
        S="n/a"
    fi
    printf 'Header Height=%s / Header Speed=%s/s / Block Height=%s / Block Speed=%s/s / Outbound Peers=%s\n' \
        "$DISPLAY_H" "$HS" "$B" "$S" "$P" | tee -a "$LOGFILE"

    last_header_stage=$header_stage
    last_height=$B
    last_headers=$H
    last_sample_time=$now

    sleep 5

done

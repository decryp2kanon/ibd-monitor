#!/usr/bin/env python3

from pathlib import Path
import csv
import json
import math
import re
from bisect import bisect_right
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone


from ibd_progress import (CheckpointProgress, STAMP, CSV_FIELDS, LogReader, backup, collector_lock,
                          latest_progress, prepare_csv, read_samples)

from ibd_connection import configuration

SCRIPT_DIR = Path(__file__).resolve().parent
DATADIR, CLI, RPC_OPTIONS, DEBUG_LOG = configuration(SCRIPT_DIR)
CSVFILE = os.environ.get("IBD_CSV", str(DATADIR / "ibd_rpc.csv"))
OUTPUT_DIR = os.path.expanduser(os.environ.get("IBD_OUTPUT_DIR", str(SCRIPT_DIR)))
MONITOR_LOGFILE = f"{OUTPUT_DIR}/graph2_monitor.log"
MAX_SAMPLE_GAP_SECONDS = 30
WAIT_FOR_DATA = 75
RELOAD_OUTPUT = f"{OUTPUT_DIR}/graph_reload.png"


def rpc(method):
    output = subprocess.check_output(
        [CLI, f'-datadir={DATADIR}', *RPC_OPTIONS, method], text=True, timeout=10,
        stderr=subprocess.DEVNULL)
    return json.loads(output)


def append_sample(row):
    # A fresh IBD run can remove/recreate the CSV while this collector stays
    # alive. Recheck its header before every append (the collector lock is held).
    prepare_csv(CSVFILE, DEBUG_LOG)
    with Path(CSVFILE).open('a', newline='') as out:
        csv.DictWriter(out, fieldnames=CSV_FIELDS).writerow(row)


def monitor_loop(once=False):
    lock = collector_lock(CSVFILE)
    if lock is None:
        print('An IBD collector is already running:', CSVFILE, flush=True)
        return
    try:
        prepare_csv(CSVFILE, DEBUG_LOG)
        log = LogReader(DEBUG_LOG)
        while True:
            try:
                # A renamed/recreated datadir must not leave an old collector
                # writing through the same pathname with an obsolete lock.
                held = os.fstat(lock.fileno())
                current = os.stat(str(CSVFILE) + '.monitor.lock')
                if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
                    print('Collector path changed; releasing old monitor', flush=True)
                    return
                chain, peers, net = rpc('getblockchaininfo'), rpc('getpeerinfo'), rpc('getnettotals')
                row = dict(zip(CSV_FIELDS[:6], [datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    chain['blocks'], chain['headers'],
                    sum(not p.get('inbound', False) for p in peers),
                    sum(len(p.get('inflight', [])) for p in peers), net['totalbytesrecv']]))
                row.update(log.poll())
                append_sample(row)
                print(json.dumps(row), flush=True)
            except Exception as error:
                if once:
                    raise
                print(f'RPC/progress error: {error}', flush=True)
            if once:
                return
            time.sleep(5)
    finally:
        lock.close()


def monitor_is_running():
    lock = collector_lock(CSVFILE)
    if lock is None:
        return True
    lock.close()
    return False


def ensure_monitor_running():
    if monitor_is_running():
        return
    csv_path = Path(CSVFILE)
    previous_size = csv_path.stat().st_size if csv_path.exists() else 0
    backup(MONITOR_LOGFILE)
    log_file = open(MONITOR_LOGFILE, "a")
    subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--monitor"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log_file.close()
    for _ in range(50):
        if csv_path.exists() and csv_path.stat().st_size > previous_size:
            return
        time.sleep(0.2)


def ibd_is_complete():
    try:
        chain = rpc("getblockchaininfo")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"IBD status unavailable: {error}", file=sys.stderr, flush=True)
        return False
    return isinstance(chain, dict) and chain.get("initialblockdownload") is False


def close_viewer(process):
    # Only close the viewer started by this reload process.
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def reload_loop(interval):
    render_command = [sys.executable, str(Path(__file__).resolve()), "--render-reload"]
    final_render_command = [
        sys.executable, str(Path(__file__).resolve()), "--render-complete"
    ]

    feh_process = None
    try:
        while True:
            result = subprocess.run(render_command, check=False)
            if result.returncode == 0:
                break
            if result.returncode != WAIT_FOR_DATA:
                raise SystemExit(result.returncode)
            time.sleep(interval)
        feh_process = subprocess.Popen(
            ["feh", "--scale-down", "--reload", str(interval), RELOAD_OUTPUT],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        next_render = time.monotonic() + interval
        while True:
            if ibd_is_complete():
                # Do not finalize an old image if the final render fails.
                result = subprocess.run(final_render_command, check=False)
                if result.returncode == 0 and Path(RELOAD_OUTPUT).is_file():
                    close_viewer(feh_process)
                    complete_output = str(Path(OUTPUT_DIR) / (
                        "graph_complete_" + datetime.now().strftime("%y-%m-%d-%H-%M-%S")
                        + ".png"
                    ))
                    os.rename(RELOAD_OUTPUT, complete_output)
                    subprocess.Popen(
                        ["feh", "--scale-down", complete_output],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                    print("IBD complete:", complete_output, flush=True)
                    return
            time.sleep(max(0, next_render - time.monotonic()))
            subprocess.run(render_command, check=False)
            next_render += interval
            if next_render < time.monotonic():
                next_render = time.monotonic() + interval
    except KeyboardInterrupt:
        pass
    finally:
        if feh_process is not None:
            close_viewer(feh_process)


def presync_header_plot(times, x, header_heights, debug_log, speed_samples=None, speed_points=None):
    """Keep presync drawing intact and optionally collect existing log-rate samples."""
    if speed_samples is not None:
        speed_samples.extend([None] * len(times))
    stamps = [t.timestamp() for t in times]
    events = []
    visioneye = False
    has_presync = False
    pattern = re.compile(r'Pre-synchronizing blockheaders, height: ([0-9]+)\b')
    try:
        with Path(debug_log).open(errors='replace') as source:
            for line in source:
                if not line.endswith('\n'):
                    continue
                reset = (' version ' in line or 'Shutdown:' in line)
                checkpoint = 'checkpoint header' in line.lower()
                match = pattern.search(line)
                synchronizing = 'Synchronizing blockheaders, height:' in line
                if not (reset or checkpoint or match or synchronizing):
                    continue
                timestamp = STAMP.match(line)
                if not timestamp:
                    continue
                stamp = datetime.fromisoformat(timestamp[1].replace('Z', '+00:00'))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                stamp = stamp.timestamp()
                if reset:
                    visioneye = False
                elif checkpoint:
                    visioneye = True
                if not stamps[0] <= stamp <= stamps[-1]:
                    continue
                if match and not visioneye:
                    events.append((stamp, int(match[1])))
                    has_presync = True
                elif reset or checkpoint or synchronizing:
                    events.append((stamp, None))
    except OSError:
        return x, header_heights
    if not has_presync:
        return x, header_heights

    # Merge display points into the existing compressed time axis. Stable order
    # preserves multiple heights logged in one second without dividing by zero.
    events.sort(key=lambda event: event[0])
    plot_x, plot_heights = [], []
    active_height = None
    progress = CheckpointProgress()
    event_index = 0
    for i, stamp in enumerate(stamps):
        while event_index < len(events) and events[event_index][0] <= stamp:
            event_time, height = events[event_index]
            left = max(0, bisect_right(stamps, event_time) - 1)
            right = min(left + 1, len(stamps) - 1)
            fraction = ((event_time - stamps[left]) / (stamps[right] - stamps[left])
                        if right != left else 0)
            event_x = x[left] + fraction * (x[right] - x[left])
            if height is not None or active_height is not None:
                baseline = header_heights[left] + fraction * (header_heights[right] - header_heights[left])
                plot_x.append(event_x)
                plot_heights.append(height if height is not None else baseline)
            if height is None:
                progress = CheckpointProgress()
            else:
                progress.record_height('presync', height, event_time, None)
                if speed_points is not None:
                    point = (event_time, event_x, progress.snapshot(event_time))
                    if (speed_points and speed_points[-1][0] == event_time and
                            speed_points[-1][2]['checkpoint_session'] == progress.session):
                        speed_points[-1] = point
                    else:
                        speed_points.append(point)
            active_height = height
            event_index += 1
        if active_height is not None and speed_samples is not None:
            speed_samples[i] = progress.snapshot(stamp)
        plot_x.append(x[i])
        plot_heights.append(header_heights[i] if active_height is None else active_height)
    return plot_x, plot_heights


def block_performance_indices(times, heights, rates, window_starts):
    """Positive measurements whose entire window follows observed block startup."""
    processing_start = None
    eligible = []
    for i in range(1, len(times)):
        interval = (times[i] - times[i - 1]).total_seconds()
        if heights[i] < heights[i - 1] or interval > MAX_SAMPLE_GAP_SECONDS:
            processing_start = None
            continue
        if interval <= 0:
            continue
        if processing_start is None and heights[i] > heights[i - 1]:
            processing_start = i
        start = window_starts[i]
        if (processing_start is not None and start >= processing_start and
                (times[i] - times[start]).total_seconds() >= 60 and
                math.isfinite(rates[i]) and rates[i] > 0):
            eligible.append(i)
    return eligible


def main():
    if sys.argv[1:] == ["--progress-json"]:
        try:
            print(json.dumps(latest_progress(CSVFILE)))
        except (OSError, ValueError, csv.Error):
            print(json.dumps({"checkpoint_phase": "unavailable"}))
        return
    if sys.argv[1:] == ["--ensure-monitor"]:
        ensure_monitor_running()
        return
    if sys.argv[1:] in (["--monitor"], ["--once"]):
        try:
            monitor_loop(once=sys.argv[1] == "--once")
        except (OSError, ValueError, csv.Error) as error:
            print(f"IBD collector: {error}", file=sys.stderr)
            raise SystemExit(1)
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--reload":
        if len(sys.argv) != 3:
            raise SystemExit("usage: ./graph.sh --reload SECONDS")
        try:
            reload_interval = int(sys.argv[2])
        except ValueError:
            raise SystemExit("reload interval must be a positive integer")
        if reload_interval <= 0:
            raise SystemExit("reload interval must be a positive integer")
        reload_loop(reload_interval)
        raise SystemExit(0)


    render_reload = len(sys.argv) == 2 and sys.argv[1] in (
        "--render-reload", "--render-complete"
    )
    render_complete = len(sys.argv) == 2 and sys.argv[1] == "--render-complete"
    if len(sys.argv) > 1 and not render_reload:
        raise SystemExit("usage: ./graph.sh [--reload SECONDS]")


    if os.environ.get("IBD_NO_MONITOR") != "1":
        ensure_monitor_running()

    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import to_rgb
    from matplotlib.ticker import FuncFormatter, MaxNLocator


    def format_rate(value):
        return f"{value:,.1f}/s" if math.isfinite(value) else "n/a"


    def format_elapsed_tick(value, _position):
        if value < 0:
            return ""

        total_seconds = int(round(value * 60))
        if total_seconds == 0:
            return "0s"
        if total_seconds < 3600:
            minutes, seconds = divmod(total_seconds, 60)
            if not minutes:
                return f"{seconds}s"
            return f"{minutes}m" + (f"{seconds}s" if seconds else "")
        total_minutes = int(round(value))

        total_hours, minutes = divmod(total_minutes, 60)
        if total_hours < 24:
            return f"{total_hours}h" + (f"{minutes}m" if minutes else "")

        total_days, hours = divmod(total_hours, 24)
        if total_days < 7:
            return f"{total_days}d" + (f"{hours}h" if hours else "")

        total_weeks, days = divmod(total_days, 7)
        if total_weeks < 7:
            return f"{total_weeks}w" + (f"{days}d" if days else "")

        # Requested display convention: 7 weeks = 1 month, 12 months = 1 year.
        total_months, weeks = divmod(total_weeks, 7)
        if total_months < 12:
            return f"{total_months}mo" + (f"{weeks}w" if weeks else "")

        years, months = divmod(total_months, 12)
        return f"{years}y" + (f"{months}mo" if months else "")


    def format_elapsed(seconds):
        hours, remainder = divmod(max(0, int(seconds)), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h{minutes:02d}m{seconds:02d}s"


    def format_delay(seconds):
        total_seconds = max(0, int(round(seconds)))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes:02d}m{seconds:02d}s"
        if minutes:
            return f"{minutes}m{seconds:02d}s"
        return f"{seconds}s"


    def format_eta(current_time, current_height, target_height, average_rate):
        if (not math.isfinite(average_rate) or average_rate <= 0 or
                target_height <= current_height):
            return "Calculating..."
        eta = current_time + timedelta(
            seconds=(target_height - current_height) / average_rate
        )
        return eta.strftime("%y-%m-%d %H:%M:%S")


    # ==========================
    # 출력 파일
    # ==========================

    if render_reload:
        OUTPUT = RELOAD_OUTPUT
        SAVE_OUTPUT = f"{RELOAD_OUTPUT}.tmp.png"
    else:
        OUTPUT = f"{OUTPUT_DIR}/graph_{datetime.now().strftime('%y%m%d_%H%M%S')}.png"
        SAVE_OUTPUT = OUTPUT



    # ==========================
    # RPC CSV 읽기
    # ==========================

    times = []
    heights = []
    header_heights = []
    peer_values = []
    checkpoint_rows = []


    try:
        csv_source = open(CSVFILE, "r", newline="")
    except FileNotFoundError:
        print("대기 중: IBD 데이터가 아직 없습니다.", flush=True)
        raise SystemExit(WAIT_FOR_DATA if render_reload else 0)

    with csv_source as f:

        reader = read_samples(f)

        for row in reader:
            sample_time = datetime.strptime(
                row["time"],
                "%Y-%m-%d %H:%M:%S"
            )
            sample_height = int(row["height"])
            sample_headers = int(row["headers"])
            sample_peers = int(row["peers"])

            # A stale monitor PID check previously allowed multiple collectors to
            # append samples for the same second. Keep the latest such sample so
            # duplicate rows do not add artificial time to the x-axis.
            if times and sample_time == times[-1]:
                heights[-1] = sample_height
                header_heights[-1] = sample_headers
                peer_values[-1] = sample_peers
                checkpoint_rows[-1] = row
                continue
            if times and sample_time < times[-1]:
                continue

            times.append(sample_time)
            heights.append(sample_height)
            header_heights.append(sample_headers)
            peer_values.append(sample_peers)
            checkpoint_rows.append(row)


    if len(heights) < 2:
        print("대기 중: IBD 데이터가 2개 이상 수집되면 그래프를 표시합니다.", flush=True)
        raise SystemExit(WAIT_FOR_DATA if render_reload else 0)


    # Preserve the original RPC header height as the common sync target before
    # checkpoint progress is substituted for the plotted header height below.
    sync_target_height = max(header_heights)
    last_progress = checkpoint_rows[-1]
    if last_progress.get('checkpoint_phase', '').startswith('core31'):
        height, percent = last_progress.get('core31_height'), last_progress.get('core31_percent')
        if height not in (None, ''):
            sync_target_height = max(sync_target_height, int(height))
            if last_progress['checkpoint_phase'] == 'core31' and percent not in (None, '') and float(percent) > 0:
                # Only the latest approximate percentage is an ETA estimate.
                # Once complete, discard estimates in favor of the RPC/log tip.
                sync_target_height = max(sync_target_height, round(int(height) * 100 / float(percent)))



    # ==========================
    # 시간축
    # ==========================

    start = times[0]
    elapsed_seconds = (times[-1] - start).total_seconds()

    # Build a continuous recorded-time axis. Long periods with no samples are not
    # useful performance data, so resume at the next normal sampling interval
    # instead of leaving a wall-clock-sized hole in the graph.
    normal_intervals = sorted(
        (times[i] - times[i - 1]).total_seconds()
        for i in range(1, len(times))
        if 0 < (times[i] - times[i - 1]).total_seconds() <= MAX_SAMPLE_GAP_SECONDS
    )
    normal_interval = normal_intervals[len(normal_intervals) // 2] if normal_intervals else 5.0

    x = [0.0]
    active_elapsed_seconds = 0.0
    for i in range(1, len(times)):
        interval = (times[i] - times[i - 1]).total_seconds()
        active_elapsed_seconds += interval if 0 < interval <= MAX_SAMPLE_GAP_SECONDS else normal_interval
        x.append(active_elapsed_seconds / 60.0)





    # ==========================
    # Rolling 60-second throughput
    # ==========================

    def rolling_rate_60s(values, sessions=None, window_starts=None):
        rates = []
        window_start = 0

        for i, current_time in enumerate(times):
            if i and ((sessions is not None and sessions[i] != sessions[i - 1]) or
                      values[i] < values[i - 1] or
                      (times[i] - times[i - 1]).total_seconds() > MAX_SAMPLE_GAP_SECONDS):
                window_start = i
            cutoff = current_time - timedelta(seconds=60)

            while (window_start + 1 < i and
                   times[window_start + 1] <= cutoff):
                window_start += 1

            if window_starts is not None:
                window_starts.append(window_start)
            elapsed = (current_time - times[window_start]).total_seconds()
            if elapsed >= 60:
                rates.append(
                    (values[i] - values[window_start]) / elapsed
                )
            else:
                rates.append(float("nan"))

        return rates


    # Choose the active stage for the two existing header metrics. Keep stage
    # boundaries out of the rate window so replay/restarts cannot create spikes.
    header_sessions = []
    for i, row in enumerate(checkpoint_rows):
        phase = row.get("checkpoint_phase")
        active = phase in ("presync", "replay")
        header_sessions.append((phase, row.get("checkpoint_session")) if active else ("headers", ""))
        if active:
            value = row.get(phase + "_height")
            header_heights[i] = int(value) if value not in (None, "") else float("nan")
        elif phase in ('core31', 'core31_complete'):
            header_sessions[-1] = ('core31', row.get('checkpoint_session'))
            value = row.get('core31_height')
            if value not in (None, ''):
                # During initial sync prefer log progress. Once caught up, RPC
                # remains authoritative and can advance beyond the logged tip.
                header_heights[i] = (int(value) if phase == 'core31' else
                                     max(header_heights[i], int(value)))

    presync_speed_samples = []
    presync_speed_points = []
    header_plot_x, header_plot_heights = presync_header_plot(
        times, x, header_heights, DEBUG_LOG, presync_speed_samples, presync_speed_points
    )
    header_speed_heights = header_heights.copy()
    header_speed_sessions = header_sessions.copy()
    for i, sample in enumerate(presync_speed_samples):
        if sample is not None:
            header_speed_heights[i] = sample['presync_height']
            header_speed_sessions[i] = ('core31_presync', sample['checkpoint_session'])

    block_window_starts = []
    block_rate_60s = rolling_rate_60s(heights, window_starts=block_window_starts)
    header_rate_60s = rolling_rate_60s(header_speed_heights, header_speed_sessions)
    for i, row in enumerate(checkpoint_rows):
        phase = row.get("checkpoint_phase")
        if presync_speed_samples[i] is not None:
            value = presync_speed_samples[i]['presync_rate']
            header_rate_60s[i] = float(value) if value not in (None, '') else float('nan')
        elif phase in ("presync", "replay"):
            value = row.get(phase + "_rate")
            header_rate_60s[i] = float(value) if value not in (None, "") else float("nan")
        elif phase == 'core31':
            value = row.get('core31_rate')
            header_rate_60s[i] = float(value) if value not in (None, '') else float('nan')
        elif phase == 'core31_complete' and int(row.get('core31_height') or 0) >= int(row['headers']):
            header_rate_60s[i] = 0.0


    def find_series_gaps(values, active):
        """Return missing spans enclosed by valid data in one active series."""
        gaps = []
        last_valid_index = None
        missing_since_last_valid = False

        for i, (value, is_active) in enumerate(zip(values, active)):
            if not is_active:
                # Never bridge a series boundary to a different sync stage.
                last_valid_index = None
                missing_since_last_valid = False
            elif math.isfinite(value):
                if last_valid_index is not None and missing_since_last_valid:
                    delay_seconds = (times[i] - times[last_valid_index]).total_seconds()
                    if delay_seconds > normal_interval * 2:
                        gap_midpoint = times[last_valid_index] + (
                            times[i] - times[last_valid_index]
                        ) / 2
                        time_fraction = (
                            (gap_midpoint - times[last_valid_index]).total_seconds()
                            / delay_seconds
                        )
                        midpoint_x = x[last_valid_index] + time_fraction * (
                            x[i] - x[last_valid_index]
                        )
                        gaps.append(
                            (midpoint_x, (values[last_valid_index] + value) / 2,
                             delay_seconds,
                             x[last_valid_index], values[last_valid_index],
                             x[i], value)
                        )
                last_valid_index = i
                missing_since_last_valid = False
            elif last_valid_index is not None:
                missing_since_last_valid = True

        return gaps


    block_sync_start_index = next(
        (i for i in range(1, len(heights)) if heights[i] > heights[i - 1]),
        None
    )
    header_active = [
        (block_sync_start_index is None or i < block_sync_start_index or
         checkpoint_rows[i].get('checkpoint_phase') == 'core31')
        for i in range(len(times))
    ]
    block_active = [
        block_sync_start_index is not None and i >= block_sync_start_index
        for i in range(len(times))
    ]
    always_active = [True] * len(times)

    first_sample_speed = {}
    for i, sample in enumerate(presync_speed_samples):
        if sample is not None and math.isfinite(header_rate_60s[i]):
            first_sample_speed.setdefault(sample['checkpoint_session'], times[i].timestamp())
    speed_records = list(zip(times, x, header_speed_heights, header_rate_60s,
                             header_active, header_speed_sessions))
    for stamp, event_x, sample in presync_speed_points:
        session = sample['checkpoint_session']
        if stamp < first_sample_speed.get(session, float('inf')):
            rate = sample['presync_rate']
            speed_records.append((datetime.fromtimestamp(stamp), event_x,
                                  sample['presync_height'],
                                  float(rate) if rate != '' else float('nan'),
                                  True, ('core31_presync', session)))
    speed_records.sort(key=lambda record: record[0])
    (header_speed_times, header_speed_x, header_summary_heights, header_plot_rates,
     header_summary_active, header_summary_sessions) = map(list, zip(*speed_records))

    series_gaps = {
        "header_height": find_series_gaps(header_heights, header_active),
        "header_speed": find_series_gaps(header_rate_60s, header_active),
        "block_height": find_series_gaps(heights, block_active),
        "block_speed": find_series_gaps(block_rate_60s, block_active),
        "peers": find_series_gaps(peer_values, always_active),
    }


    block_sync_started = block_sync_start_index is not None
    last_phase = checkpoint_rows[-1].get('checkpoint_phase')
    if last_phase == 'core31_complete' or (block_sync_started and last_phase != 'core31'):
        header_eta_text = "Headers synced"
    else:
        header_eta_text = format_eta(
            times[-1], header_heights[-1], sync_target_height, header_rate_60s[-1]
        )

    block_eta_text = format_eta(
        times[-1], heights[-1], sync_target_height, block_rate_60s[-1]
    )

    if render_complete:
        # The reload loop reached this renderer only after RPC reported
        # initialblockdownload=false. The CSV collector can lag by one sample,
        # so its last heights must not leave a stale positive ETA in the final
        # image.
        block_eta_text = datetime.now().strftime("%y-%m-%d %H:%M:%S")
        block_remaining_text = "Complete"
    elif sync_target_height <= heights[-1] and sync_target_height > 0:
        block_remaining_text = "0s"
    elif (sync_target_height > heights[-1] and
          math.isfinite(block_rate_60s[-1]) and block_rate_60s[-1] > 0):
        block_remaining_text = format_delay(
            (sync_target_height - heights[-1]) / block_rate_60s[-1]
        )
    else:
        block_remaining_text = "Calculating..."


    peer_change_indexes = [0] + [
        i for i in range(1, len(peer_values))
        if peer_values[i] != peer_values[i - 1]
    ]

    def speed_summary(values, rates, active, sessions=None, sample_times=None):
        """Summarize observed rates and throughput over valid active intervals."""
        sample_times = times if sample_times is None else sample_times
        observed_rates = []
        total_progress = 0.0
        total_seconds = 0.0
        started = False
        for i in range(1, len(sample_times)):
            if not active[i]:
                started = False
                continue
            if sessions is not None and sessions[i] != sessions[i - 1]:
                started = False
                continue
            previous, current = values[i - 1], values[i]
            if not (math.isfinite(previous) and math.isfinite(current)):
                continue
            delta = current - previous
            if delta < 0:
                started = False
                continue
            interval = (sample_times[i] - sample_times[i - 1]).total_seconds()
            if not 0 < interval <= MAX_SAMPLE_GAP_SECONDS:
                continue
            if delta > 0:
                started = True
            if not started:
                continue
            total_progress += delta
            total_seconds += interval
            if math.isfinite(rates[i]):
                observed_rates.append(rates[i])
        maximum = max(observed_rates, default=float("nan"))
        minimum = min(observed_rates, default=float("nan"))
        average = total_progress / total_seconds if total_seconds else float("nan")
        return (f"Max: {format_rate(maximum)}", f"Min: {format_rate(minimum)}",
                f"Avg: {format_rate(average)}")

    block_stat_indices = block_performance_indices(times, heights, block_rate_60s, block_window_starts)
    block_stat_rates = [block_rate_60s[i] for i in block_stat_indices]
    # All three statistics describe the same complete, positive rate samples.
    block_statistics = (
        "Max: " + format_rate(max(block_stat_rates, default=float('nan'))),
        "Min: " + format_rate(min(block_stat_rates, default=float('nan'))),
        "Avg: " + format_rate(math.fsum(block_stat_rates) / len(block_stat_rates)
                              if block_stat_rates else float('nan')),
    )

    fig, ax1 = plt.subplots(figsize=(14, 9))
    # Reserve right space for the four additional metric axes.
    fig.subplots_adjust(left=0.07, right=0.66, bottom=0.18, top=0.79)

    # Block Height 🔵

    ax_block = ax1.twinx()

    l1, = ax_block.plot(
        x,
        heights,
        color="blue",
        linewidth=2,
        label="Block Height"
    )



    # Header Height 🟣

    l2, = ax1.plot(
        header_plot_x,
        header_plot_heights,
        color="purple",
        linewidth=2,
        label="Header Height"
    )

    # Latest height labels

    ax1.annotate(
        f"Headers={header_plot_heights[-1]:,}",
        xy=(x[-1], header_plot_heights[-1]),
        xytext=(10, 0),
        textcoords="offset points",
        ha="left",
        va="center",
        color="purple",
        fontsize=10,
        fontweight="bold",
        bbox=dict(
            boxstyle="round,pad=0.25",
            facecolor="white",
            edgecolor="purple",
            alpha=0.9
        )
    )

    ax_block.annotate(
        f"Blocks={heights[-1]:,}",
        xy=(x[-1], heights[-1]),
        xytext=(10, 0),
        textcoords="offset points",
        ha="left",
        va="center",
        color="blue",
        fontsize=10,
        fontweight="bold",
        bbox=dict(
            boxstyle="round,pad=0.25",
            facecolor="white",
            edgecolor="blue",
            alpha=0.9
        )
    )


    ax1.set_xlabel(
        "Elapsed Time"
    )
    ax1.xaxis.set_major_formatter(
        FuncFormatter(format_elapsed_tick)
    )

    # Around seven labeled ticks, including seconds for short measurements.
    ax1.set_xlim(0, max(x[-1], 1.0 / 60.0))
    ax1.xaxis.set_major_locator(MaxNLocator(nbins=7, min_n_ticks=4, steps=[1, 2, 2.5, 5, 10]))
    ax1.tick_params(axis="x", labelbottom=True, pad=5)

    ax1.set_ylabel(
        "Header Height (M headers)",
        color="purple"
    )
    ax1.tick_params(axis="y", colors="purple")
    ax1.spines["left"].set_color("purple")
    ax1.yaxis.set_major_formatter(
        FuncFormatter(lambda value, _: f"{value / 1_000_000:g}M")
    )

    # Use the same height scale for blocks and headers so their progress is
    # directly comparable.
    ax_block.set_ylim(ax1.get_ylim())
    ax_block.set_ylabel(
        "Block Height (M blocks)",
        color="blue"
    )
    ax_block.tick_params(axis="y", colors="blue")
    ax_block.spines["right"].set_color("blue")
    ax_block.yaxis.set_major_formatter(
        FuncFormatter(lambda value, _: f"{value / 1_000_000:g}M")
    )



    # Speed 🔴

    ax2 = ax1.twinx()
    ax2.spines["right"].set_position(
        ("axes", 1.12)
    )


    l3, = ax2.plot(
        x,
        block_rate_60s,
        color="red",
        linewidth=2,
        label="Block Speed: " + format_rate(block_rate_60s[-1])
    )

    ax2.set_ylabel(
        "Block/s",
        color="red"
    )
    ax2.tick_params(axis="y", colors="red")


    # Header speed 🟠

    ax_header_speed = ax1.twinx()
    ax_header_speed.spines["right"].set_position(
        ("axes", 1.24)
    )

    l5, = ax_header_speed.plot(
        header_speed_x,
        header_plot_rates,
        color="orange",
        linewidth=2,
        label="Header Speed: " + format_rate(header_rate_60s[-1])
    )

    ax_header_speed.set_ylabel(
        "Header/s",
        color="darkorange"
    )
    ax_header_speed.tick_params(axis="y", colors="darkorange")


    # Outbound peers 🟢

    ax_peers = ax1.twinx()
    ax_peers.spines["right"].set_position(
        ("axes", 1.36)
    )

    for i in peer_change_indexes:
        ax_peers.text(
            x[i],
            peer_values[i],
            str(peer_values[i]),
            ha="center",
            va="center",
            color="green",
            fontsize=7,
            fontweight="bold"
        )

    l4 = ax_peers.scatter(
        [],
        [],
        color="green",
        marker="$N$",
        s=7,
        label=f"Outbound Peers: {peer_values[-1]}"
    )

    max_peer_value = max(peer_values)
    ax_peers.set_ylim(0, max_peer_value + 1)
    ax_peers.set_yticks(range(1, max_peer_value + 1))
    ax_peers.set_ylabel(
        "Outbound Peers",
        color="green"
    )
    ax_peers.tick_params(axis="y", colors="green")


    gap_labels = []
    for axis, gaps in (
        (ax1, series_gaps["header_height"]),
        (ax_header_speed, series_gaps["header_speed"]),
        (ax_block, series_gaps["block_height"]),
        (ax2, series_gaps["block_speed"]),
        (ax_peers, series_gaps["peers"]),
    ):
        for gap in gaps:
            gap_labels.append((axis,) + gap)



    # Legend

    ax1.grid(alpha=0.20)
    legend_lines = [l2, l5, l1, l3, l4]
    statistic_rows = {
        1: speed_summary(header_summary_heights, header_plot_rates, header_summary_active,
                         header_summary_sessions, header_speed_times),
        3: block_statistics,
    }
    # Matplotlib fills legend columns first. Blank handles keep each statistic
    # aligned with its metric label while preserving the original first row.
    from matplotlib.lines import Line2D
    blank_handle = Line2D([], [], linestyle="none", marker="")
    legend_handles = []
    legend_labels = []
    from matplotlib.font_manager import FontProperties
    legend_font = FontProperties(size=10)
    legend_renderer = fig.canvas.get_renderer()
    label_width = 100 * fig.dpi / 72.0
    def value_text_width(value):
        value_math = value.replace(",", "{,}")
        return legend_renderer.get_text_width_height_descent(
            rf"$\mathtt{{{value_math}}}$", legend_font, ismath=True
        )[0]

    # Keep the existing tab stop as the field's left edge; share a right edge
    # across all statistic rows, expanding only for values longer than 4,477.6/s.
    value_width = max(
        [value_text_width("0,000.0/s")] + [
            value_text_width(text.split(": ", 1)[1])
            for column, rows in statistic_rows.items()
            for text in (legend_lines[column].get_label(), *rows)
        ]
    )
    for column, height in ((0, header_plot_heights[-1]), (2, heights[-1])):
        current = f"{height:,.0f}" if math.isfinite(height) else "n/a"
        statistic_rows[column] = (f"Now: {current}", "", "")
    for column, line in enumerate(legend_lines):
        legend_handles.extend([line, blank_handle, blank_handle, blank_handle])
        rows = [line.get_label(), *statistic_rows.get(column, ("", "", ""))]
        if column in statistic_rows or column == 4:
            # A measured text tab stop keeps proportional labels aligned with
            # monospaced values within each legend string.
            aligned_rows = []
            for row, text in enumerate(rows):
                if ": " not in text:
                    aligned_rows.append(text)
                    continue
                name, value = text.split(": ", 1)
                label = name if row == 0 else name + ":"
                label_math = r"\mathdefault{" + label.replace(" ", r"\ ") + "}"
                width, _, _ = legend_renderer.get_text_width_height_descent(
                    "$" + label_math + "$", legend_font, ismath=True
                )
                # Height columns only need a short Now: label; keep the existing
                # speed tab stops unchanged and the legend inside the image.
                tab_width = 32 * fig.dpi / 72.0 if column in (0, 2) else label_width
                field_width = max(value_width, value_text_width(value))
                gap = (tab_width - width + field_width - value_text_width(value)) / (
                    10 * fig.dpi / 72.0
                )
                if column == 4:
                    gap = 1.0
                value_math = value.replace(",", "{,}")
                aligned_rows.append(
                    "$" + label_math + rf"\hspace{{{gap:.4f}}}\mathtt{{{value_math}}}$"
                )
            rows = aligned_rows
        legend_labels.extend(rows)
    legend = fig.legend(
        legend_handles, legend_labels,
        loc="upper center", bbox_to_anchor=(0.5, 0.94), fontsize=10, ncol=5
    )
    for text in legend.get_texts():
        text.set_color("black")
    # Fresh IBD may have no indexed headers/blocks yet. Avoid misleading negative
    # heights and scientific-notation near-zero axes while checkpoint work runs.
    for axis, values in ((ax1, header_plot_heights), (ax_block, heights)):
        if max(values) == 0:
            axis.set_ylim(0, 1_000_000)
    for axis, values in ((ax2, block_rate_60s), (ax_header_speed, header_rate_60s)):
        if not any(math.isfinite(value) and value > 0 for value in values):
            axis.set_ylim(0, 1)
    fig.suptitle("Sugarchain IBD (Initial Block Download) Progress", y=0.975, fontsize=13)

    # Center the complete plot, including the extra right axes and their labels,
    # without changing the plotting area's width or height.
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    plot_bounds = [axis.get_tightbbox(renderer) for axis in fig.axes]
    left_edge = min(bounds.x0 for bounds in plot_bounds)
    right_edge = max(bounds.x1 for bounds in plot_bounds)
    horizontal_shift = (fig.bbox.x0 + fig.bbox.width / 2
                        - (left_edge + right_edge) / 2) / fig.bbox.width
    fig.subplots_adjust(
        left=fig.subplotpars.left + horizontal_shift,
        right=fig.subplotpars.right + horizontal_shift
    )


    # Measurement status below the x-axis. Keep each pair in one compact box and
    # align the ETA box with the right edge of the main plot.
    status_box = dict(
        boxstyle="round,pad=0.25",
        facecolor="white",
        edgecolor="black",
        alpha=0.9
    )
    fig.text(
        ax1.get_position().x0, 0.09,
        f"Start: {start.strftime('%y-%m-%d %H:%M:%S')}\n"
        f"Elapsed: {format_elapsed(elapsed_seconds)}",
        ha="left", va="bottom", fontsize=10, fontweight="bold",
        linespacing=1.0, bbox=status_box
    )
    fig.text(
        ax1.get_position().x1, 0.09,
        f"Header ETA: {header_eta_text}\n"
        f"Block ETA: {block_eta_text}\n"
        f"Time until fully synced: {block_remaining_text}",
        ha="left", va="bottom", fontsize=10, fontweight="bold",
        linespacing=1.0, bbox=status_box
    )


    fig.canvas.draw()


    def choose_gap_label_color(axis, x_value, y_value):
        """Choose readable text color from the pixels around a gap label."""
        candidates = (
            "#000000",  # black
            "#263238",  # charcoal
            "#005A5B",  # deep teal
            "#7A1F3D",  # burgundy
            "#4D5B00",  # dark olive
            "#312E81",  # deep indigo
        )
        pixels = np.asarray(fig.canvas.buffer_rgba())[:, :, :3] / 255.0
        display_x, display_y = axis.transData.transform((x_value, y_value))
        display_y += 6 * fig.dpi / 72.0
        center_x = int(round(display_x))
        center_y = pixels.shape[0] - 1 - int(round(display_y))
        x0, x1 = max(0, center_x - 35), min(pixels.shape[1], center_x + 36)
        y0, y1 = max(0, center_y - 12), min(pixels.shape[0], center_y + 13)
        nearby = pixels[y0:y1, x0:x1].reshape(-1, 3)

        # Ignore the white background and very light grid pixels; retain nearby
        # graph lines and text that the label must remain distinct from.
        nearby = nearby[np.min(nearby, axis=1) < 0.82]
        if not len(nearby):
            return candidates[0]

        def luminance(colors):
            linear = np.where(
                colors <= 0.04045,
                colors / 12.92,
                ((colors + 0.055) / 1.055) ** 2.4,
            )
            return linear @ np.array([0.2126, 0.7152, 0.0722])

        nearby_luminance = luminance(nearby)
        best_color = candidates[0]
        best_score = float("-inf")
        for candidate in candidates:
            rgb = np.array(to_rgb(candidate))
            candidate_luminance = luminance(rgb.reshape(1, 3))[0]
            contrast = np.maximum(
                (candidate_luminance + 0.05) / (nearby_luminance + 0.05),
                (nearby_luminance + 0.05) / (candidate_luminance + 0.05),
            )
            color_distance = np.linalg.norm(nearby - rgb, axis=1)
            # Use a low percentile so the chosen color remains distinct from more
            # than just the single most favorable pixel in a mixed-color area.
            score = np.percentile(contrast + 2.0 * color_distance, 10)
            if score > best_score:
                best_color = candidate
                best_score = score
        return best_color


    for (axis, midpoint_x, midpoint_value, delay_seconds,
         start_x, start_value, end_x, end_value) in gap_labels:
        label_color = choose_gap_label_color(axis, midpoint_x, midpoint_value)
        axis.annotate(
            format_delay(delay_seconds),
            xy=(midpoint_x, midpoint_value),
            xytext=(0, 6),
            textcoords="offset points",
            ha="center",
            va="center",
            color=label_color,
            fontsize=9,
            fontweight="bold"
        )
        axis.plot(
            [start_x, end_x],
            [start_value, end_value],
            linestyle="none",
            marker="o",
            markersize=5,
            markerfacecolor="none",
            markeredgecolor=label_color,
            markeredgewidth=1.2,
            zorder=10
        )


    backup(OUTPUT)
    plt.savefig(
        SAVE_OUTPUT,
        dpi=100,
        facecolor="white"
    )

    if render_reload:
        os.replace(SAVE_OUTPUT, OUTPUT)



    print("saved:", OUTPUT)
    print("Header Height:", header_plot_heights[-1])
    print("Header Speed:", header_rate_60s[-1], "/s")
    print("Block Height:", heights[-1])
    print("Block Speed:", block_rate_60s[-1], "/s")
    print("Outbound Peers:", peer_values[-1])


    # feh 자동 실행

    if not render_reload and os.environ.get("IBD_NO_VIEWER") != "1":
        subprocess.Popen(
            [
                "feh",
                "--scale-down",
                OUTPUT
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )


if __name__ == "__main__":
    main()

"""Read checkpoint progress from core logs without relying on indexed headers."""
import csv
import fcntl
import os
import re
import shutil
import tempfile
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

BASE_FIELDS = ['time', 'height', 'headers', 'peers', 'inflight', 'bytes_recv']
PROGRESS_FIELDS = ['checkpoint_phase', 'presync_height', 'presync_rate',
                   'replay_height', 'replay_rate', 'checkpoint_session', 'checkpoint_log_time']
LEGACY_FIELDS = BASE_FIELDS + PROGRESS_FIELDS
CORE31_FIELDS = ['core31_height', 'core31_rate', 'core31_percent']
PROGRESS_FIELDS += CORE31_FIELDS
CSV_FIELDS = BASE_FIELDS + PROGRESS_FIELDS
STAMP = re.compile(r'^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)')
START = re.compile(r'(Starting|Resuming) checkpoint header (authentication|replay) from height=(\d+) peer=(\d+)')
HEIGHT = re.compile(r'Checkpoint header (presync|replay) height=(\d+) peer=(\d+)')

CORE31_HEIGHT = re.compile(r'Synchronizing blockheaders, height: ([0-9]+)(?: \(~([0-9]+(?:\.[0-9]+)?)%\))?')


def backup(path):
    path = Path(path)
    destination = Path(str(path) + '.bak260925')
    if path.exists() and not destination.exists():
        # Exclusive creation: never replace an earlier backup.
        with destination.open('xb') as out, path.open('rb') as source:
            shutil.copyfileobj(source, out)
        shutil.copystat(path, destination)
    return destination


class CheckpointProgress:
    def __init__(self):
        self.phase = 'none'
        self.session = ''
        self.peer = None
        self.height = None
        self.stamp = None
        self.start = None
        self.points = deque()
        self.core31_percent = None
        self.visioneye = False

    def begin(self, phase, height, stamp, peer):
        self.phase, self.height, self.stamp, self.peer = phase, height, stamp, peer
        self.session = f'{stamp}:{peer}:{phase}'
        self.points.clear()
        if height is not None:
            self.points.append((stamp, height))

    def record_height(self, phase, height, stamp, peer):
        """Record a parsed header sample using the existing log rate window."""
        if phase != self.phase or peer != self.peer or (self.height is not None and height < self.height):
            self.begin(phase, height, stamp, peer)
        else:
            self.height, self.stamp = height, stamp
            if self.points and stamp == self.points[-1][0]:
                self.points[-1] = (stamp, height)
            else:
                self.points.append((stamp, height))
            while len(self.points) > 2 and self.points[1][0] <= stamp - 60:
                self.points.popleft()

    def feed(self, line):
        match = STAMP.match(line)
        if not match:
            return
        stamp = datetime.fromisoformat(match[1].replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)  # Core debug.log uses UTC.
        stamp = stamp.timestamp()
        if ' version ' in line or 'Shutdown:' in line:
            self.__init__()
            return
        match = CORE31_HEIGHT.search(line)
        if match and not self.visioneye:
            height = int(match[1])
            percent = float(match[2]) if match[2] is not None else None
            if percent is not None and not 0 <= percent <= 100:
                return
            if self.phase not in ('core31', 'core31_complete') or self.height is None or height < self.height:
                self.begin('core31', height, stamp, None)
            elif self.stamp is not None and stamp < self.stamp:
                return
            else:
                self.height, self.stamp = height, stamp
                if self.points and stamp == self.points[-1][0]:
                    self.points[-1] = (stamp, height)
                else:
                    self.points.append((stamp, height))
                while len(self.points) > 2 and self.points[1][0] <= stamp - 60:
                    self.points.popleft()
            self.core31_percent = percent
            self.phase = 'core31_complete' if percent == 100 else 'core31'
            return
        match = START.search(line)
        if match:
            self.visioneye = True
            phase = 'presync' if match[2] == 'authentication' else 'replay'
            self.start = int(match[3]) if phase == 'presync' else None
            self.begin(phase, int(match[3]), stamp, match[4])
            return
        if 'Checkpoint header commitments authenticated; replaying' in line:
            self.visioneye = True
            self.begin('replay', self.start, stamp, self.peer)
            return
        match = HEIGHT.search(line)
        if match:
            self.visioneye = True
            phase, height, peer = match[1], int(match[2]), match[3]
            self.record_height(phase, height, stamp, peer)
            return
        if 'Checkpoint header replay complete' in line:
            self.phase = 'complete'
        elif ('Checkpoint header authentication failed' in line or
              'Timeout authenticating checkpoint headers' in line or
              (self.peer is not None and re.search(r'(?:disconnecting|disconnect).*peer=' + self.peer + r'\b', line))):
            self.phase = 'waiting'

    def snapshot(self, now=None):
        now = datetime.now().timestamp() if now is None else now
        result = dict.fromkeys(PROGRESS_FIELDS, '')
        result.update(checkpoint_phase=self.phase, checkpoint_session=self.session)
        if self.stamp is not None:
            result['checkpoint_log_time'] = datetime.fromtimestamp(self.stamp).isoformat(sep=' ', timespec='seconds')
        if self.phase not in ('presync', 'replay', 'core31', 'core31_complete') or self.height is None:
            return result
        phase = 'core31' if self.phase.startswith('core31') else self.phase
        result[phase + '_height'] = self.height
        if phase == 'core31':
            result['core31_percent'] = self.core31_percent if self.core31_percent is not None else ''
        if self.phase == 'core31_complete':
            result['core31_rate'] = 0.0
            return result
        # A rate is measured between actual log heights, not repeated RPC samples.
        # Stalled logs must not retain a positive speed indefinitely.
        if now - self.stamp > 60:
            result[phase + '_rate'] = 0.0
        elif len(self.points) >= 2:
            elapsed = self.points[-1][0] - self.points[0][0]
            if elapsed > 0:
                result[phase + '_rate'] = round((self.points[-1][1] - self.points[0][1]) / elapsed, 2)
        return result


class LogReader:
    def __init__(self, path):
        self.path = Path(path)
        self.identity = None
        self.offset = 0
        self.anchor = b''
        self.progress = CheckpointProgress()

    def poll(self):
        try:
            with self.path.open('rb') as source:
                stat = os.fstat(source.fileno())
                identity = (stat.st_dev, stat.st_ino)
                # Also detect truncate-and-regrow / inode reuse between polls.
                source.seek(max(0, self.offset - len(self.anchor)))
                previous_bytes = source.read(len(self.anchor))
                if (identity != self.identity or stat.st_size < self.offset or
                        previous_bytes != self.anchor):
                    self.progress = CheckpointProgress()
                    self.offset = 0
                    self.identity = identity
                    self.anchor = b''
                source.seek(self.offset)
                for line in source:
                    if not line.endswith(b'\n'):
                        break  # Re-read partial writes on the next poll.
                    self.progress.feed(line.decode('utf-8', errors='replace'))
                    self.offset += len(line)
                source.seek(max(0, self.offset - 128))
                self.anchor = source.read(min(128, self.offset))
        except OSError:
            return dict.fromkeys(PROGRESS_FIELDS, '') | {'checkpoint_phase': 'unavailable'}
        return self.progress.snapshot()


def log_events(path):
    try:
        with Path(path).open(errors='replace') as source:
            for line in source:
                if 'checkpoint' not in line.lower() and ' version ' not in line and 'Shutdown:' not in line and 'Synchronizing blockheaders' not in line:
                    continue
                match = STAMP.match(line)
                if match:
                    stamp = datetime.fromisoformat(match[1].replace('Z', '+00:00'))
                    if stamp.tzinfo is None:
                        stamp = stamp.replace(tzinfo=timezone.utc)
                    yield stamp.timestamp(), line
    except FileNotFoundError:
        return


def sample_row(values):
    """Decode either supported record layout, even without a file header."""
    if not values or not any(values) or values[0].lstrip("\ufeff") == "time":
        return None
    if len(values) not in (len(BASE_FIELDS), len(LEGACY_FIELDS), len(CSV_FIELDS)):
        raise ValueError(f"Invalid IBD CSV record: expected 6, 13 or 16 columns, got {len(values)}")
    row = dict.fromkeys(CSV_FIELDS, '')
    row.update(zip(CSV_FIELDS, values))
    row['time'] = row['time'].lstrip("\ufeff")
    datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S')
    for field in BASE_FIELDS[1:]:
        int(row[field])
    for field in ('presync_height', 'replay_height', 'core31_height'):
        if row[field]:
            int(row[field])
    for field in ('presync_rate', 'replay_rate', 'core31_rate', 'core31_percent'):
        if row[field]:
            float(row[field])
    return row


def read_samples(source):
    """Headers may be absent or repeated after a collector restart."""
    for values in csv.reader(source):
        row = sample_row(values)
        if row is not None:
            yield row


def prepare_csv(path, debug_log, repair=False):
    """Upgrade six-column CSV, preserving rows and backfilling available logs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open('w', newline='') as out:
            csv.writer(out).writerow(CSV_FIELDS)
        return
    with path.open(newline='') as source:
        first = next(csv.reader(source), [])
    if first == CSV_FIELDS and not repair:
        return
    if first not in (BASE_FIELDS, LEGACY_FIELDS, CSV_FIELDS) and (not first or not STAMP.match(first[0])):
        raise ValueError('Unrecognized CSV header; original file left untouched')
    backup(path)
    # Preserve this particular pre-repair state as well as the original backup.
    revision = Path(str(path) + '.bak260925.repair-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    with revision.open('xb') as out, path.open('rb') as source:
        shutil.copyfileobj(source, out)
    shutil.copystat(path, revision)
    progress = CheckpointProgress()
    events = iter(log_events(debug_log))
    event = next(events, None)
    temp = None
    try:
        with path.open(newline='') as source, tempfile.NamedTemporaryFile(mode='w', newline='', dir=path.parent, delete=False) as out:
            temp = out.name
            writer = csv.DictWriter(out, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in read_samples(source):
                now = datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S').timestamp()
                while event is not None and event[0] <= now:
                    progress.feed(event[1])
                    event = next(events, None)
                snapshot = progress.snapshot(now)
                if (not row.get("checkpoint_phase") or
                        (row.get("checkpoint_phase") in ('none', 'unavailable') and
                         snapshot['checkpoint_phase'].startswith('core31'))):
                    row.update(snapshot)
                writer.writerow(row)
        os.chmod(temp, path.stat().st_mode & 0o777)
        os.replace(temp, path)
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)


def collector_lock(csv_path):
    path = Path(str(csv_path) + '.monitor.lock')
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open('a')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def latest_progress(path):
    # A bounded tail read works for both headerless and normal CSVs. Never use
    # a data row as dictionary keys, or consume a partly written last record.
    with Path(path).open('rb') as source:
        source.seek(0, 2)
        size = source.tell()
        offset = max(0, size - 8192)
        source.seek(offset)
        data = source.read()
    lines = data.splitlines(keepends=True)
    if offset and lines:
        lines.pop(0)
    for line in reversed(lines):
        if not line.endswith(b'\n'):
            continue
        try:
            row = sample_row(next(csv.reader([line.decode('utf-8')])))
        except (ValueError, UnicodeError, csv.Error):
            continue
        if row is not None:
            age = datetime.now().timestamp() - datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S').timestamp()
            if age > 30:
                return {'checkpoint_phase': 'unavailable'}
            return row
    return {'checkpoint_phase': 'unavailable'}

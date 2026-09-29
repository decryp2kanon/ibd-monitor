"""Terminal-only Core31 presync adapter; shared graph modules stay unchanged."""
from datetime import datetime, timezone
import re

from ibd_progress import CheckpointProgress, LogReader, STAMP


class TerminalProgress(CheckpointProgress):
    def feed(self, line):
        height = re.search(r'Pre-synchronizing blockheaders, height: ([0-9]+)\b', line)
        timestamp = STAMP.match(line)
        if height and timestamp and not self.visioneye:
            stamp = datetime.fromisoformat(timestamp[1].replace('Z', '+00:00'))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            stamp = stamp.timestamp()
            if self.stamp is not None and stamp < self.stamp:
                return
            # Reuse the existing rate window, same-second coalescing and phase
            # resets. The shared parser already handles Core31 replay.
            self.record_height('presync', int(height[1]), stamp, None)
        else:
            super().feed(line)


class TerminalLogReader(LogReader):
    # LogReader creates fresh parsers on initialization/rotation. Adapt only
    # this reader instance; never replace the shared module's parser class.
    @property
    def progress(self):
        return self._progress

    @progress.setter
    def progress(self, parser):
        self._progress = TerminalProgress()
        self._progress.__dict__.update(parser.__dict__)

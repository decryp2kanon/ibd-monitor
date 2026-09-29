"""Offline terminal integration test. No node, collector or live datadir used."""
import os
from pathlib import Path
import select
import signal
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from ibd_terminal_progress import TerminalLogReader, TerminalProgress
from ibd_progress import CheckpointProgress


class TerminalProgressTest(unittest.TestCase):
    def test_presync_replay_completion_without_graph_csv(self):
        source = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('ibd_test.sh', 'ibd_progress.py', 'ibd_connection.py', 'ibd_terminal_progress.py'):
                (root / name).write_bytes((source / name).read_bytes())
            # Collector is deliberately absent; progress must come from the log.
            cli = root / 'git/sugarchain/src/sugarchain-cli'
            cli.parent.mkdir(parents=True)
            cli.write_text('#!/bin/bash\ncase "${!#}" in\n'
                           'getblockchaininfo) echo \'{"blocks":0,"headers":0}\';;\n'
                           'getpeerinfo) echo \'[{"inbound":false}]\';;\nesac\n')
            cli.chmod(0o755)
            data = root / 'data'
            data.mkdir()
            log = data / 'debug.log'

            def progress(label, first, last, percent):
                now = datetime.now(timezone.utc)
                with log.open('a') as stream:
                    for stamp, height in ((now - timedelta(seconds=1), first), (now, last)):
                        stream.write(f'{stamp.isoformat().replace("+00:00", "Z")} '
                                     f'{label} blockheaders, height: {height} (~{percent}%)\n')

            progress('Pre-synchronizing', 2000, 4000, '0.01')
            env = dict(os.environ, HOME=str(root), IBD_DATADIR=str(data), IBD_CLI=str(cli),
                       IBD_LOGDIR=str(root), PYTHONDONTWRITEBYTECODE='1')
            env.pop('IBD_DEBUG_LOG', None)
            process = subprocess.Popen(['bash', str(root / 'ibd_test.sh')], env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, start_new_session=True)
            try:
                def line():
                    self.assertTrue(select.select([process.stdout], [], [], 15)[0], 'terminal stalled')
                    result = process.stdout.readline()
                    self.assertIn('Block Height=0', result)
                    self.assertIn('Outbound Peers=1', result)
                    return result

                self.assertIn('Header Height=4000 / Header Speed=2000/s', line())
                progress('Synchronizing', 2000, 8000, '0.02')
                self.assertIn('Header Height=8000 / Header Speed=6000/s', line())
                progress('Synchronizing', 9000, 10000, '100.00')
                self.assertIn('Header Height=10000 / Header Speed=0/s', line())
                self.assertFalse((data / 'ibd_rpc.csv').exists())
            finally:
                os.killpg(process.pid, signal.SIGINT)
                try:
                    process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)
                    self.fail('temporary terminal reader did not exit')

    def test_shared_graph_parser_is_unchanged(self):
        shared, terminal = CheckpointProgress(), TerminalProgress()
        line = '2026-09-30T00:00:00Z Pre-synchronizing blockheaders, height: 2000'
        shared.feed(line)
        terminal.feed(line)
        self.assertEqual(shared.phase, 'none')
        self.assertEqual(terminal.phase, 'presync')

    def test_same_second_phase_reset_restart_and_rotation(self):
        def row(second, height, prefix='Pre-synchronizing'):
            return f'2026-09-30T00:00:{second:02d}Z {prefix} blockheaders, height: {height}\n'
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'debug.log'
            log.write_text(row(0, 2000) + row(1, 4000) + row(2, 6000) + row(2, 8000))
            reader = TerminalLogReader(log)
            reader.poll()
            stamp = reader.progress.stamp
            sample = reader.progress.snapshot(stamp)
            self.assertEqual(sample['presync_rate'], 3000)
            self.assertEqual(reader.progress.snapshot(stamp + 61)['presync_rate'], 0)
            with log.open('a') as out:
                out.write(row(3, 2000, 'Synchronizing'))
            reader.poll()
            self.assertEqual(reader.progress.snapshot(reader.progress.stamp)['core31_rate'], '')
            with log.open('a') as out:
                out.write(row(4, 8000, 'Synchronizing'))
            reader.poll()
            self.assertEqual(reader.progress.snapshot(reader.progress.stamp)['core31_rate'], 6000)
            with log.open('a') as out:
                out.write('2026-09-30T00:00:05Z Sugarchain Komorebi version v31.1\n')
                out.write(row(6, 2000))
            reader.poll()
            self.assertEqual(reader.progress.snapshot(reader.progress.stamp)['presync_rate'], '')
            log.rename(log.with_suffix('.old'))
            log.write_text(row(7, 1000) + row(8, 3000))
            reader.poll()
            self.assertEqual(reader.progress.snapshot(reader.progress.stamp)['presync_rate'], 2000)

    def test_visioneye_rate_is_preserved(self):
        p = TerminalProgress()
        p.feed('2026-09-30T00:00:00Z Starting checkpoint header authentication from height=0 peer=4')
        p.feed('2026-09-30T00:00:02Z Checkpoint header presync height=4000 peer=4')
        self.assertEqual(p.snapshot(p.stamp)['presync_rate'], 2000)
        p.feed('2026-09-30T00:00:03Z Checkpoint header commitments authenticated; replaying peer=4')
        p.feed('2026-09-30T00:00:05Z Checkpoint header replay height=8000 peer=4')
        self.assertEqual(p.snapshot(p.stamp)['replay_rate'], 4000)


if __name__ == '__main__':
    unittest.main()

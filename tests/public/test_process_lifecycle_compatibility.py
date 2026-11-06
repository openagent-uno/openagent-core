"""Legacy host startup must preserve real, independently owned processes."""
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import warnings

from openagent_core.core.serve_singleton import find_stale_serve_pids, kill_stale_serve_processes


class ProcessLifecycleCompatibility(unittest.TestCase):
    def test_real_desktop_launcher_and_other_server_are_never_signalled(self):
        with tempfile.TemporaryDirectory(prefix='runtime-lifecycle-') as temporary:
            directory = Path(temporary)
            for arguments in (
                ['openagent-desktop.py', '--server-num=101', '--auth-file='+str(directory/'authority')],
                ['openagent', 'serve', str(directory)],
            ):
                with self.subTest(arguments=arguments):
                    child = subprocess.Popen([sys.executable, '-u', '-c',
                        "import time; print('ready', flush=True); time.sleep(30)", *arguments],
                        stdout=subprocess.PIPE, text=True)
                    try:
                        self.assertEqual(child.stdout.readline().strip(), 'ready')
                        with warnings.catch_warnings(record=True) as notices:
                            warnings.simplefilter('always')
                            with patch.object(os, 'kill', side_effect=AssertionError('Foreign process signal')):
                                self.assertEqual(find_stale_serve_pids(directory), [])
                                self.assertEqual(kill_stale_serve_processes(directory, grace_seconds=0), [])
                            self.assertEqual(len(notices), 2)
                        self.assertIsNone(child.poll(), 'An independently owned process was terminated')
                    finally:
                        child.terminate()
                        child.wait(timeout=5)
                        child.stdout.close()


if __name__ == '__main__':
    unittest.main()

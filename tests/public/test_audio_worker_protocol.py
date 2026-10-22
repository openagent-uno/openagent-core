"""The audio worker owns one JSON pipe through vendor shutdown, not just await."""
from pathlib import Path
import json
import subprocess
import sys
import unittest

from openagent_core import audio_worker


# Only provider IO is scripted. The actual public worker main and OS descriptors
# run in a fresh interpreter, including its real logging/thread/atexit shutdown.
CHILD = r'''
import atexit,asyncio,json,logging,os,sys,threading,subprocess
sys.path.insert(0,sys.argv[1])
from openagent_core import audio_worker
saved_logger=logging.Logger('fixture-vendor')
saved_logger.addHandler(logging.StreamHandler(sys.stdout))
saved_logger.setLevel(logging.INFO)
def noisy_shutdown():
 def late_logs():
  print('vendor-atexit-stdout')
  print('vendor-atexit-original',file=sys.__stdout__)
  print('vendor-atexit-stderr',file=sys.stderr)
  saved_logger.info('vendor-atexit-preexisting-handler')
  os.write(1,b'vendor-atexit-native-fd1\n')
  os.write(2,b'vendor-atexit-native-fd2\n')
 t=threading.Thread(target=late_logs);t.start();t.join()
atexit.register(noisy_shutdown)
async def provider_io(request):
 print('vendor-during-call')
 saved_logger.info('vendor-during-call-preexisting-handler')
 await asyncio.sleep(0)
 subprocess.run([sys.executable,'-I','-c',"import os\nfor fd in range(3,16):\n try:os.write(fd,b'vendor-grandchild-inherited-pipe')\n except OSError:pass"],close_fds=False,check=True)
 if request.get('operation')=='fail':raise ValueError('fixture-private-configuration')
 return {'text':'Il quarzo sette è già pronto.'}
audio_worker.execute=provider_io
print('vendor-buffered-before-main',end='')
audio_worker.main()
'''


class AudioWorkerProtocol(unittest.TestCase):
    def invoke(self, request):
        result=subprocess.run([sys.executable,'-I','-c',CHILD,
            str(Path(audio_worker.__file__).resolve().parents[1])],input=request,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=8,check=True)
        # Complete output is parsed, never split or trimmed to hide vendor data.
        body=json.loads(result.stdout)
        self.assertEqual(b'',result.stderr)
        self.assertNotIn(b'vendor-',result.stdout)
        self.assertNotIn(b'fixture-private',result.stdout)
        return body

    def test_shutdown_thread_atexit_cached_handlers_and_native_fds_cannot_pollute_success(self):
        self.assertEqual({'text':'Il quarzo sette è già pronto.'},self.invoke(b'{"operation":"transcribe"}'))

    def test_shutdown_logs_cannot_pollute_or_disclose_provider_errors(self):
        self.assertEqual({'error':'audio_provider_unavailable'},self.invoke(b'{"operation":"fail"}'))

    def test_invalid_and_oversize_input_keep_the_single_error_protocol(self):
        for request in (b'{invalid',b'x'*262145):
            with self.subTest(bytes=len(request)):
                self.assertEqual({'error':'audio_provider_unavailable'},self.invoke(request))

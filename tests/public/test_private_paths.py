import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from openagent_core.private_paths import require_private_directory, require_private_file


class PrivatePaths(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        if os.name == 'nt':
            # Only this fresh synthetic fixture is changed, never a host path.
            self.windows_acl(self.root)

    def windows_acl(self, path, extra=None):
        env = {**os.environ, 'PRIVATE_FIXTURE_PATH':str(path)}
        code = '''$ErrorActionPreference='Stop'
$p=$env:PRIVATE_FIXTURE_PATH
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$sd='O:'+ $sid +'D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;'+$sid+')' '''
        if extra:
            code += "\n$sd += '" + extra + "'"
        code += '''
$a=Get-Acl -LiteralPath $p
$a.SetSecurityDescriptorSddlForm($sd)
Set-Acl -LiteralPath $p -AclObject $a
'''
        result = subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',code],env={k:v for k,v in env.items() if k.upper()!='PSMODULEPATH'},capture_output=True)
        self.assertEqual(result.returncode,0,'Synthetic ACL setup failed')

    def test_actual_private_directory_and_open_file(self):
        require_private_directory(self.root)
        with (self.root/'state').open('xb') as file:
            os.chmod(file.name,0o600)
            require_private_file(file.fileno())
        if os.name == 'nt':
            # Python synthesises these bits despite the exact private DACL.
            self.assertTrue(self.root.stat().st_mode & 0o022)

    @unittest.skipUnless(os.name=='posix','native POSIX permission contract')
    def test_posix_broad_directory_and_file_are_rejected(self):
        self.root.chmod(0o722)
        with self.assertRaises(ValueError):require_private_directory(self.root)
        self.root.chmod(0o700)
        path=self.root/'state';path.write_text('neutral');path.chmod(0o644)
        with path.open('rb') as file:
            with self.assertRaises(ValueError):require_private_file(file.fileno())

    def test_nonregular_file_and_relative_directory_are_rejected(self):
        with self.assertRaises((ValueError,OSError)):require_private_directory(Path('.'))
        path=self.root/'state';path.write_text('neutral');path.chmod(0o600)
        with self.assertRaises(ValueError):require_private_directory(path)

    @unittest.skipUnless(os.name=='posix','unprivileged link creation')
    def test_directory_link_is_rejected(self):
        link=self.root/'link';link.symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(ValueError):require_private_directory(link)

    @unittest.skipUnless(os.name=='nt','actual Windows DACL contract')
    def test_foreign_mutation_is_rejected_for_directories(self):
        self.windows_acl(self.root,'(A;OICI;GW;;;BU)')
        with self.assertRaises(ValueError):require_private_directory(self.root)
        self.windows_acl(self.root)

    @unittest.skipUnless(os.name=='nt','actual opened-file DACL contract')
    def test_foreign_read_is_rejected_on_the_opened_file(self):
        path=self.root/'state';path.write_text('neutral')
        self.windows_acl(path,'(A;;GR;;;BU)')
        with path.open('rb') as file:
            with self.assertRaises(ValueError):require_private_file(file.fileno())
        self.windows_acl(path)

if __name__=='__main__':unittest.main()

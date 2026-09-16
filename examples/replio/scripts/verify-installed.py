"""Run reference-consumer acceptance tests with installed packages only."""
from importlib import metadata, util
from pathlib import Path
import shutil
import site
import sys
import tempfile
import unittest

roots=[Path(path).resolve() for path in site.getsitepackages()]
for module,distribution in [('openagent_core','openagent-core'),
        ('openagent_storage_sqlite','openagent-storage-sqlite'),
        ('replio_agent_example','replio-agent-example')]:
    specification=util.find_spec(module)
    if specification is None or specification.origin is None:
        raise RuntimeError(f'{distribution} is not installed')
    origin=Path(specification.origin).resolve()
    if not any(origin.is_relative_to(root) for root in roots):
        raise RuntimeError(f'{module} resolves outside installed packages: {origin}')
    print(f'{distribution}=={metadata.version(distribution)}: {origin}')

with tempfile.TemporaryDirectory(prefix='replio-wheel-tests-') as temporary:
    tests=Path(temporary)/'tests'
    shutil.copytree(Path(__file__).resolve().parents[1]/'tests',tests)
    suite=unittest.defaultTestLoader.discover(str(tests))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

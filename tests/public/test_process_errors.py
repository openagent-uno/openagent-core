from openagent_core.process_errors import configure_subprocess_error_mode


def test_windows_subprocess_error_policy_preserves_existing_bits():
    class Kernel:
        mode = 0x8004
        def GetErrorMode(self): return self.mode
        def SetErrorMode(self, mode): self.mode = mode
    kernel = Kernel()
    configure_subprocess_error_mode(kernel=kernel)
    assert kernel.mode == 0x8007
    configure_subprocess_error_mode(kernel=kernel)
    assert kernel.mode == 0x8007

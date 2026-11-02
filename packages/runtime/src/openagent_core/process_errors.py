"""Noninteractive subprocess error policy for an explicitly selected engine host."""
import sys


def configure_subprocess_error_mode(*, kernel=None):
    """Prevent critical-error/fault dialogs from hanging inherited Windows children.

    This is a process-wide additive policy, not a temporary per-thread change.
    Child processes inherit it; existing error-mode bits are preserved. It does
    not replace the transport's cleanup Job Object or the host's sandbox.
    """
    if kernel is None:
        if sys.platform != 'win32':
            return
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetErrorMode.restype = wintypes.UINT
        kernel.SetErrorMode.argtypes = [wintypes.UINT]
        kernel.SetErrorMode.restype = wintypes.UINT
    kernel.SetErrorMode(kernel.GetErrorMode() | 0x0001 | 0x0002)

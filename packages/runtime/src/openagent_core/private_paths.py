"""Read-only platform permission checks for host-owned private state.

Windows permission bits describe DOS attributes, not the DACL. Inspect the
actual descriptor and process principals there; POSIX keeps its native modes.
These functions never grant access, change ownership or repair permissions.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat


def require_private_directory(path: Path) -> None:
    """Reject links, non-directories and mutation authority for other principals."""
    path = Path(path)
    info = path.lstat()
    if not path.is_absolute() or path.resolve() != path or not stat.S_ISDIR(info.st_mode):
        raise ValueError('Private state requires a literal absolute directory')
    if os.name == 'nt':
        _windows_path(path)
    elif info.st_mode & 0o022:
        raise ValueError('Private directory grants mutation to other principals')


def require_private_file(descriptor: int) -> None:
    """Validate the already-opened file, before reading or writing its content."""
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('Private state requires a regular file')
    if os.name == 'nt':
        import msvcrt
        _windows_handle(msvcrt.get_osfhandle(descriptor), directory=False)
    elif info.st_mode & 0o077:
        raise ValueError('Private file grants access to other principals')


def _windows_api():
    import ctypes as c
    from ctypes import wintypes as w
    kernel = c.WinDLL('kernel32', use_last_error=True)
    security = c.WinDLL('advapi32', use_last_error=True)
    signatures = [
        (kernel, 'GetCurrentProcess', [], w.HANDLE),
        (kernel, 'CloseHandle', [w.HANDLE], w.BOOL),
        (kernel, 'LocalFree', [c.c_void_p], c.c_void_p),
        (kernel, 'CreateFileW', [w.LPCWSTR, w.DWORD, w.DWORD, c.c_void_p, w.DWORD, w.DWORD, w.HANDLE], w.HANDLE),
        (kernel, 'GetFileInformationByHandleEx', [w.HANDLE, c.c_int, c.c_void_p, w.DWORD], w.BOOL),
        (security, 'OpenProcessToken', [w.HANDLE, w.DWORD, c.POINTER(w.HANDLE)], w.BOOL),
        (security, 'GetTokenInformation', [w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.POINTER(w.DWORD)], w.BOOL),
        (security, 'IsValidSid', [c.c_void_p], w.BOOL),
        (security, 'GetLengthSid', [c.c_void_p], w.DWORD),
        (security, 'ConvertSidToStringSidW', [c.c_void_p, c.POINTER(w.LPWSTR)], w.BOOL),
        (security, 'GetSecurityInfo', [w.HANDLE, c.c_int, w.DWORD, c.POINTER(c.c_void_p), c.c_void_p, c.POINTER(c.c_void_p), c.c_void_p, c.POINTER(c.c_void_p)], w.DWORD),
        (security, 'GetAclInformation', [c.c_void_p, c.c_void_p, w.DWORD, c.c_int], w.BOOL),
        (security, 'GetAce', [c.c_void_p, w.DWORD, c.POINTER(c.c_void_p)], w.BOOL),
    ]
    for library, name, arguments, result in signatures:
        function = getattr(library, name)
        function.argtypes, function.restype = arguments, result
    return c, w, kernel, security


def _windows_path(path):
    c, _, kernel, _ = _windows_api()
    # READ_CONTROL | FILE_READ_ATTRIBUTES, share all, OPEN_EXISTING,
    # BACKUP_SEMANTICS | OPEN_REPARSE_POINT. No handle follows the final link.
    handle = kernel.CreateFileW(str(path), 0x20080, 7, None, 3, 0x02200000, None)
    if handle in (None, c.c_void_p(-1).value):
        raise ValueError('Private directory security cannot be inspected')
    try:
        _windows_handle(handle, directory=True)
    finally:
        kernel.CloseHandle(handle)


def _windows_handle(handle, *, directory):
    c, w, kernel, security = _windows_api()

    def sid_text(pointer):
        if not pointer or not security.IsValidSid(pointer):
            raise ValueError('Private state has an invalid security principal')
        value = w.LPWSTR()
        if not security.ConvertSidToStringSidW(pointer, c.byref(value)):
            raise ValueError('Private state security principal cannot be inspected')
        try:
            return value.value
        finally:
            kernel.LocalFree(c.cast(value, c.c_void_p))

    class Attributes(c.Structure):
        _fields_ = [('attributes', w.DWORD), ('tag', w.DWORD)]

    attributes = Attributes()
    if not kernel.GetFileInformationByHandleEx(handle, 9, c.byref(attributes), c.sizeof(attributes)):
        raise ValueError('Private state attributes cannot be inspected')
    if attributes.attributes & 0x400 or bool(attributes.attributes & 0x10) != directory:
        raise ValueError('Private state cannot be a reparse point or a different file kind')

    token = w.HANDLE()
    if not security.OpenProcessToken(kernel.GetCurrentProcess(), 8, c.byref(token)):
        raise ValueError('Private state process authority cannot be inspected')
    try:
        def token_data(kind):
            size = w.DWORD()
            security.GetTokenInformation(token, kind, None, 0, c.byref(size))
            if not size.value or size.value > 65536:
                raise ValueError('Private state token information cannot be inspected')
            buffer = c.create_string_buffer(size.value)
            if not security.GetTokenInformation(token, kind, buffer, size.value, c.byref(size)):
                raise ValueError('Private state token information cannot be inspected')
            return buffer

        def token_sid(kind):
            buffer = token_data(kind)
            return sid_text(c.cast(buffer, c.POINTER(c.c_void_p))[0])

        user = token_sid(1)
        app_info = token_data(29)
        is_app = c.cast(app_info, c.POINTER(w.DWORD))[0]
        if is_app not in (0, 1):
            raise ValueError('Private state process kind cannot be inspected')
        package = token_sid(31) if is_app else None
        # Privileged OS administrators already control process and storage.
        trusted = {user, 'S-1-5-18', 'S-1-5-32-544'}
        if package is not None:
            trusted.add(package)
    finally:
        kernel.CloseHandle(token)

    owner, acl, descriptor = c.c_void_p(), c.c_void_p(), c.c_void_p()
    result = security.GetSecurityInfo(handle, 1, 5, c.byref(owner), None, c.byref(acl), None, c.byref(descriptor))
    if result:
        raise ValueError('Private state descriptor cannot be inspected')
    try:
        if sid_text(owner) not in trusted or not acl:
            raise ValueError('Private state has a foreign owner or unrestricted DACL')

        class Size(c.Structure):
            _fields_ = [('count', w.DWORD), ('used', w.DWORD), ('free', w.DWORD)]

        size = Size()
        if not security.GetAclInformation(acl, c.byref(size), c.sizeof(size), 2) or size.count > 1024:
            raise ValueError('Private state DACL cannot be inspected')
        # Includes DELETE, WRITE_DAC and WRITE_OWNER, not merely data writes.
        forbidden = 0x500D0156 if directory else 0xF00D01FF
        for index in range(size.count):
            ace = c.c_void_p()
            if not security.GetAce(acl, index, c.byref(ace)):
                raise ValueError('Private state access entry cannot be inspected')
            kind, flags, length = c.c_ubyte.from_address(ace.value).value, c.c_ubyte.from_address(ace.value + 1).value, c.c_ushort.from_address(ace.value + 2).value
            if kind not in (0, 1) or length < 16:
                raise ValueError('Private state has an unsupported access entry')
            if kind == 1 or (flags & 8 and not directory):
                continue
            mask = c.c_uint32.from_address(ace.value + 4).value
            principal = sid_text(ace.value + 8)
            if 8 + security.GetLengthSid(ace.value + 8) > length:
                raise ValueError('Private state access entry has an invalid size')
            if principal not in trusted and mask & forbidden:
                raise ValueError('Private state grants access to other principals')
    finally:
        kernel.LocalFree(descriptor)

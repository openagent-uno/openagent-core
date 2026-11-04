# Native private state permissions

`openagent_core.private_paths.require_private_directory(path)` and
`require_private_file(descriptor)` validate host-owned state before content IO.
The directory must be absolute, literal and regular. An opened file descriptor
binds validation to the actual regular file that the caller will use.

On POSIX the directory rejects group/other mutation and the file rejects all
group/other access, preserving native modes. Windows mode bits describe DOS
attributes and cannot establish DACL privacy. Windows checks the actual handle,
rejects reparse points, null DACLs, foreign ownership and unsupported ACE kinds,
and evaluates allow entries against the process user, SYSTEM, administrators
and only the current AppContainer SID. Directory mutation includes deleting
children, changing the DACL and taking ownership. Files additionally deny
foreign content reads and execution. Inheritance-only grants are considered
for directories so their future state does not silently inherit broad writers.
Deny entries never justify a broad allow entry. Failure to inspect fails closed.

These are read-only checks. They do not grant access, modify ACLs, chown/chmod,
create identities or select a sandbox. A host supplies its own protected storage
root, opens files without following links, checks the descriptor before reading
or writing, and retains its domain authorization and atomic publication policy.

Neutral tests use real temporary files on POSIX and native Windows DACLs,
including foreign mutation/read counterexamples. This verifies the library
contract, independently of any embedding host's runtime qualification.

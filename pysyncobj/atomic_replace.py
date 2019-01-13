import ctypes
import os
import sys

if hasattr(ctypes, 'windll'):     # pragma: no cover
    CreateTransaction = ctypes.windll.ktmw32.CreateTransaction
    CommitTransaction = ctypes.windll.ktmw32.CommitTransaction
    MoveFileTransacted = ctypes.windll.kernel32.MoveFileTransactedW
    CloseHandle = ctypes.windll.kernel32.CloseHandle

    MOVEFILE_REPLACE_EXISTING = 0x1
    MOVEFILE_WRITE_THROUGH = 0x8

    if sys.version_info >= (3, 0):
        unicode = str

    def atomic_replace(old_path, new_path):
        if not isinstance(old_path, unicode):
            old_path = unicode(old_path, sys.getfilesystemencoding())
        if not isinstance(new_path, unicode):
            new_path = unicode(new_path, sys.getfilesystemencoding())
        ta = CreateTransaction(None, 0, 0, 0, 0, 1000, 'atomic_replace')
        if ta == -1:
            return False
        res = MoveFileTransacted(old_path, new_path, None, None,
                                 MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH, ta)
        if not res:
            CloseHandle(ta)
            return False
        res = CommitTransaction(ta)
        CloseHandle(ta)
        return bool(res)
else:
    atomicReplace = os.rename

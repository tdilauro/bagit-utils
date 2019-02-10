import contextlib
import os
import stat
import sys
import tempfile

@contextlib.contextmanager
def safe_overwrite(filepath, deleteOnFailure=True, prefix='', suffix='.tmp', text=True):
    dir = os.path.dirname(filepath)
    filestatus = os.stat(filepath)
    uid = filestatus.st_uid
    gid = filestatus.st_gid
    mode = stat.S_IMODE(filestatus.st_mode)

    tmp_fd, tmp_filepath = tempfile.mkstemp(dir=dir, prefix=prefix, suffix=suffix, text=text)

    try:
        with os.fdopen(tmp_fd, 'w' if text else 'wb') as f:
            os.chown(tmp_filepath, uid, gid)
            os.chmod(tmp_filepath, mode)
            yield f
        os.rename(tmp_filepath, filepath)
        tmp_filepath = None
    finally:
        if deleteOnFailure and (tmp_filepath is not None):
                os.unlink(tmp_filepath)

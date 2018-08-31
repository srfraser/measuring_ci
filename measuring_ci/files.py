
from contextlib import ExitStack, contextmanager
import s3fs

@contextmanager
def open_wrapper(filename, *args, **kwargs):
    with ExitStack() as stack:
        if filename.startswith('s3://'):
            fs = s3fs.S3FileSystem()
            f = stack.enter_context(fs.open(filename, *args, **kwargs))
        else:
            f = stack.enter_context(open(filename, *args, **kwargs))
        yield f

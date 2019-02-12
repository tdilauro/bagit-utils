from __future__ import print_function, unicode_literals

import collections
import os


WalkerItem = collections.namedtuple('WalkerItem', ['depth', 'type', 'path'])


def walk_dir(dir, onerror=None, followlinks=False, max_depth=None, _depth=0):
    root, subdirs, files = next(os.walk(dir, onerror=onerror, followlinks=followlinks))

    yield WalkerItem(path=os.path.abspath(root), depth=_depth, type='dir')

    _depth += 1
    if max_depth is not None and _depth > max_depth:
        return

    for file in files:
        yield WalkerItem(path=os.path.abspath(os.path.join(root, file)), depth=_depth, type='file')
    for subdir in subdirs:
        # loop as alternative to "yield from ..." for Python versions pre-3.3
        for item in walk_dir(os.path.join(root, subdir), onerror=onerror, followlinks=followlinks,
                             max_depth=max_depth, _depth=_depth):
            yield item

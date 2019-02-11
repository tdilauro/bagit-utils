from __future__ import print_function, unicode_literals

import collections
import os
import stat


payload_relative = 'data'

top = '.'
top_absolute = os.path.abspath(top)
start = os.path.join(top, payload_relative)
start = "."


def main():
    for path in walk_dir(start):
        print(path)
        pass


WalkerItem = collections.namedtuple('WalkerItem', ['path', 'depth', 'type'])


def walk_dir(dir, topdown=True, onerror=None, followlinks=False, max_depth=None, _depth=0):
    for root, subdirs, files in os.walk(dir, topdown=topdown, onerror=onerror, followlinks=followlinks
                                        ):
        # print("root:", root, "subdirs:", subdirs, "files:", files)
        yield WalkerItem(path=os.path.abspath(root), depth=_depth, type='dir')

        _depth += 1
        if max_depth is not None and _depth > max_depth:
            return

        for file in files:
            yield WalkerItem(path=os.path.abspath(os.path.join(root, file)), depth=_depth, type='file')
        for subdir in subdirs:
            walk_dir(os.path.join(root, subdir), topdown=topdown, onerror=onerror, followlinks=followlinks,
                     max_depth=max_depth, _depth=_depth)


if __name__=='__main__':
    main()

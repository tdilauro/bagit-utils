#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function, unicode_literals

import argparse
from bag_updater import Bag
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', type=int, dest='processes', default=1,
                        help='Use multiple processes to calculate checksums faster (default: %(default)s)')
    parser.add_argument('directories', nargs='+', help='one or more BagIt directories')
    args = parser.parse_args()

    processes = args.processes

    for bag_dir in args.directories:
        bag = Bag(bag_dir)
        bag.validate(processes=processes,)

        # our renamer is a generator
        rename_map = {old: new for old, new in rename_files(bag.payload_files(), basedir=bag.path)}

        # update the manifests
        bag.update_payload_filenames(rename_map=rename_map)

        # re-open and validate the update bag
        bag = bag.refresh()
        bag.validate(processes=processes, )


def rename_files(files_to_rename, basedir='', institution='jhu', interfield_sep='_', intrafield_sep='-'):
    previously = successes = failures = 0
    collection = intrafield_sep.join(os.path.basename(basedir).split(intrafield_sep)[0:2])
    for filepath in files_to_rename:
        if os.path.basename(filepath).startswith(institution + interfield_sep):
            previously += 1
        else:
            dir_tree = filepath.split(os.sep)
            new_basename = interfield_sep.join([institution, collection, intrafield_sep.join(dir_tree[-2:])])
            dir_tree[-1] = new_basename
            new_filepath = os.sep.join(dir_tree)
            # print("- renaming '%s' to '%s'..." % (filepath, new_filepath), end='')
            if fs_rename(filepath, new_filepath, basedir=basedir, dry_run=False):
                successes += 1
                # print('success.')
                # yield the old and new path only on success
                yield filepath, new_filepath
            else:
                failures += 1
                # print('failure.')
    print('summary filesystem renaming: successes: %d, failures: %d, previously renamed: %d'
          % (successes, failures, previously))


def fs_rename(old, new, basedir='', dry_run=False):
    old = os.path.join(basedir, old)
    new = os.path.join(basedir, new)
    if old != new and os.path.exists(old) and not os.path.exists(new):
        if not dry_run:
            os.rename(old, new)
        return True
    return False


if __name__=='__main__':
    main()

#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function, unicode_literals

import argparse
from bag_updater import Bag
from collections import OrderedDict
import csv
from datetime import datetime
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--dry-run', dest='dry_run', action='store_true',
                        help="don't actually make any changes")
    parser.add_argument('-m', '--map', dest='map', action='store_true',
                        help="output the mapping from old filename to new filename")
    parser.add_argument('--map-file', dest='map_file', help='file to receive mapping from old filename to new filename')
    parser.add_argument('--processes', dest='processes', type=int, default=1,
                        help='Use multiple processes to calculate checksums faster (default: %(default)s)')
    parser.add_argument('directories', nargs='+', help='one or more BagIt directories')
    args = parser.parse_args()

    processes = args.processes

    for bag_dir in args.directories:
        bag = Bag(bag_dir)
        bag_short_name = os.path.basename(bag.path)
        bag.validate(processes=processes,)

        # our renamer is a generator
        payload_files = sorted(bag.payload_files())
        rename_map = OrderedDict((old, new) for old, new
                                 in rename_files(payload_files, basedir=bag.path, dry_run=args.dry_run))

        if args.map or args.map_file is not None:
            if args.map_file is not None:
                map_file = args.map_file
            else:
                map_file = 'renameLog-' + bag_short_name + '-' + datetime.now().strftime('%Y%m%dT%H%M%S') + '.csv'
            print("Printing rename map to file '%s'" % map_file)
            emit_rename_map(rename_map, filename=map_file, type='csv')

        if not args.dry_run:
            # update the manifests
            bag.update_payload_filenames(rename_map=rename_map)

            # re-open and validate the update bag
            bag = bag.refresh()
            bag.validate(processes=processes, )


def emit_rename_map(rename_map, filename=None, type='csv'):
    f = csv.writer(open(filename, 'wb'))
    with open(filename, 'wb') as f:
        writer = csv.writer(f)
        for old, new in rename_map.items():
            writer.writerow([old, new])


def rename_files(files_to_rename, basedir='', institution='jhu', interfield_sep='_', intrafield_sep='-', dry_run=False):
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
            if fs_rename(filepath, new_filepath, basedir=basedir, dry_run=dry_run):
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

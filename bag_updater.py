#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function, unicode_literals

import bagit
import collections
import hashlib
import logging
import os
import re
from safe_overwrite import safe_overwrite

MODULE_NAME = 'bagit_updater' if __name__ == '__main__' else __name__

LOGGER = logging.getLogger(MODULE_NAME)

Manifest = collections.namedtuple('Manifest', ['type', 'algorithm', 'path', 'relpath'])
MANIFEST_FILENAME_PATTERN = re.compile(r'\A(?P<type>(\S+))-(?P<algorithm>(\S+))\.txt\Z')
MANIFEST_ENTRY_PATTERN = re.compile(r'\A(?P<hash>(\S+))(?P<spaces>(\s+))(?P<filename>(\S.*))\Z')


class Bag (bagit.Bag):

    def refresh(self):
        return self.__class__(self.path)

    def update_payload_filenames(self, rename_map=None, payload_manifests=None, tag_manifests=None):
        """OVERALL PROCESS
            given a map of old-to-new filenames (rename_map)
            get list of tagmanifest-*.txt files and associated hash algorithm(s)
            for each manifest-<algorithm>.txt file
                setup hash update callback for each tagmanifest algorithm
                rename filepaths from old to new (checksums don't change)
                snapshot each of the new hash(es) of the manifest
            for each tagmanifest-<algorithm>.txt file
                update checksums in the given <algorithm> for each of the manifest files previously listed
        """
        bag_dir = self.path
        if rename_map is None or len(rename_map) == 0:
            return
        if payload_manifests is None:
            payload_manifests = self.manifest_files()
        if tag_manifests is None:
            tag_manifests = self.tagmanifest_files()

        payload_manifest_map = {pmf.algorithm: pmf for pmf in self.manifest_objects(payload_manifests)}
        tag_manifest_map = {tmf.algorithm: tmf for tmf in self.manifest_objects(tag_manifests)}
        tag_algorithms = tag_manifest_map.keys()

        # for each tagmanifest algorithm, the mapping from each payload manifest to its hash by that algorithm
        tag_rehash_map = {alg: {} for alg in tag_algorithms}

        # update the file paths in the payload manifests
        for pmf_alg, pmf in payload_manifest_map.items():
            # setup the hash accumulaters and the associated updater callbacks
            hashes = [hashlib.new(tmf_alg) for tmf_alg in tag_algorithms]
            hash_updaters = [h.update for h in hashes]
            # update the manifest file with the new filenames
            update_payload_manifest_filepaths(os.path.join(bag_dir, pmf.relpath), new_filenames=rename_map,
                                              write_callbacks=hash_updaters)
            for h in hashes:
                tag_rehash_map[h.name].update({pmf.relpath: h.hexdigest()})

        # update the payload manifest hashes in the tag manifests
        for tmf_alg, tmf in tag_manifest_map.items():
            update_tag_manifest_hashes(os.path.join(bag_dir, tmf.relpath), new_hashes=tag_rehash_map[tmf_alg])

    def manifest_objects(self, manifest_files, pattern=None):
        if pattern is None:
            pattern = MANIFEST_FILENAME_PATTERN
        for mfile in manifest_files:
            yield Manifest(path=os.path.abspath(mfile), relpath=os.path.relpath(mfile, start=self.path),
                        **dict(pattern.match(os.path.basename(mfile)).groupdict()))


def update_payload_manifest_filepaths(manifest_file, new_filenames=None, line_pattern=None, write_callbacks=None):
    if new_filenames is None or len(new_filenames) == 0:
        return
    if line_pattern is None:
        line_pattern = MANIFEST_ENTRY_PATTERN
    with open(manifest_file, 'r') as manifest, safe_overwrite(manifest_file, text=True) as new_manifest:
        for line in manifest:
            match = line_pattern.match(line.rstrip("\n"))
            if match is None:
                output_line = line
            else:
                matched = match.groupdict()
                filename = matched['filename']
                matched['filename'] = new_filenames.get(filename, filename)
                output_line = "%(hash)s%(spaces)s%(filename)s\n" % matched
            if write_callbacks is not None:
                for callback in write_callbacks:
                    callback(output_line)
            new_manifest.write(output_line)


def update_tag_manifest_hashes(manifest_file, new_hashes=None, line_pattern=None, write_callbacks=None):
    if new_hashes is None or len(new_hashes) == 0:
        return
    if line_pattern is None:
        line_pattern = MANIFEST_ENTRY_PATTERN
    with open(manifest_file, 'r') as manifest, safe_overwrite(manifest_file, text=True) as new_manifest:
        for line in manifest:
            match = line_pattern.match(line.rstrip("\n"))
            if match is None:
                output_line = line
            else:
                matched = match.groupdict()
                hash = matched['hash']
                filename = matched['filename']
                matched['hash'] = new_hashes.get(filename, hash)
                output_line = "%(hash)s%(spaces)s%(filename)s\n" % matched
            if write_callbacks is not None:
                for callback in write_callbacks:
                    callback(output_line)
            new_manifest.write(output_line)

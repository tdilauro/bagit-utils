from __future__ import print_function, unicode_literals

import collections
import glob
import hashlib
import os
import re
from safe_overwrite import safe_overwrite
import tempfile
from walker import walk_dir

MANIFEST_LINE_PATTERN = re.compile(r'\A(?P<hash>(\S+))(?P<spaces>(\s+))(?P<filename>(\S.*))\Z')
# attributes of Manifest named tuple must include at least all match named groups in MANIFEST_FILENAME_PATTERN
# PAYLOAD_MANIFEST_FILENAME_GLOB and TAG_MANIFEST_FILENAME_GLOB must match MANIFEST_FILENAME_PATTERN
PAYLOAD_MANIFEST_FILENAME_GLOB = 'manifest-*.txt'
TAG_MANIFEST_FILENAME_GLOB = 'tagmanifest-*.txt'
MANIFEST_FILENAME_PATTERN = re.compile(r'\A(?P<filename>((?P<type>(\S+))-(?P<algorithm>(\S+))\.txt))\Z')
Manifest = collections.namedtuple('Manifest', ['filename', 'type', 'algorithm'])

# get map of old-to-new filenames (rename_map)
# get list of tagmanifest-*.txt files
# parse to determine hash algorithm of each
# for each manifest-<algorithm>.txt file
#     setup hash update callback for each tagmanifest file
#     rename filepaths from old to new (checksums don't change)
#     save hashes
# for each tagmanifest-<algorithm>.txt file
#     update checksums in the given <algorithm> for each of the manifest files previously listed
#

def main():
    ### begin TESTING ONLY
    bag = './csvs.test'
    rename_map = {
        'data/old_name_1': 'data/new_name_1',
        'data/old_name_2': 'data/new_name_2',
    }
    for old_name, new_name in {os.path.join(bag, k): os.path.join(bag, v) for k, v in rename_map.items()}.items():
        if old_name != new_name and os.path.exists(new_name) and not os.path.exists(new_name):
            os.rename(old_name, new_name)
    ### end TESTING ONLY
    manifest_updater(bag, rename_map=rename_map)


def manifest_updater(bag, rename_map=None):
    payload_manifests = {pmf.algorithm: pmf for pmf in get_payload_manifests(bag)}
    tag_manifests = {tmf.algorithm: tmf for tmf in get_tag_manifests(bag)}
    tag_algorithms = tag_manifests.keys()
    # will hold for each tagmanifest algorithm, the mapping from each payload manifest to its hash by that algorithm
    tag_rehash_map = {alg: {} for alg in tag_algorithms}
    for pmf_alg, pmf in payload_manifests.items():
        # setup the hash accumulaters and the associated updater callbacks
        hashes = [hashlib.new(tmf_alg) for tmf_alg in tag_algorithms]
        hash_updaters = [h.update for h in hashes]
        # update the manifest file with the new filenames
        update_payload_manifest_filepaths(bag, pmf.filename, new_filenames=rename_map, write_callbacks=hash_updaters)
        [tag_rehash_map[h.name].update({pmf.filename: h.hexdigest()}) for h in hashes]
    for tmf_alg, tmf in tag_manifests.items():
        update_tag_manifest_hashes(bag, tmf.filename, new_hashes=tag_rehash_map[tmf_alg])


def get_manifests(bag, fileglob=None, pattern=None):
    if fileglob is None:
        fileglob = PAYLOAD_MANIFEST_FILENAME_GLOB
    if pattern is None:
        pattern = MANIFEST_FILENAME_PATTERN
    results = [Manifest(**dict(pattern.match(os.path.basename(manifest)).groupdict()))
               for manifest in glob.glob(os.path.join(bag, fileglob))]
    return results

def get_payload_manifests(bag, fileglob=PAYLOAD_MANIFEST_FILENAME_GLOB, pattern=MANIFEST_FILENAME_PATTERN):
    return get_manifests(bag, fileglob=fileglob, pattern=pattern)

def get_tag_manifests(bag, fileglob=TAG_MANIFEST_FILENAME_GLOB, pattern=MANIFEST_FILENAME_PATTERN):
    return get_manifests(bag, fileglob=fileglob, pattern=pattern)




# todo: need a mechanism to build the hashes, as the manifest is updated.
# todo: that means that we need to know which tagmanifest algorithms are in use.
def update_payload_manifest_filepaths(bag, manifest_file, new_filenames=None, line_pattern=None, write_callbacks=None):
    if new_filenames is None or len(new_filenames) == 0:
        return
    if line_pattern is None:
        line_pattern = MANIFEST_LINE_PATTERN
    manifest_file = os.path.join(bag, manifest_file)
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


def update_tag_manifest_hashes(bag, manifest_file, new_hashes=None, line_pattern=None, write_callbacks=None):
    if new_hashes is None or len(new_hashes) == 0:
        return
    if line_pattern is None:
        line_pattern = MANIFEST_LINE_PATTERN
    manifest_file = os.path.join(bag, manifest_file)
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

if __name__=='__main__':
    main()

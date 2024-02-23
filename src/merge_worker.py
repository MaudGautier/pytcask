"""MVP Features of the worker:
- do_merge method => merge files until max file size + creates a hint file

do_merge method:
- merge files until max size, then creates new files
- adds a hint file
- MERGING: file should not be read while it's being processed, then should add it and then delete others (OK because never re-read)
- Timestamp should be equal to that of the most recent key merged

For now: merge all files of the store
TODO later: add options to merge only partly (between timestamps for example)

Unclear thoughts:
- should merge file size be the same as storage_engine max file size ?
"""

import os

from src.io_handling import File, MergedFile, ReadableFile, ImmutableFile
from src.storage_engine import StorageEngine


class MergeWorker:
    DEFAULT_MAX_FILE_SIZE = 150

    def __init__(
        self,
        storage_engine: StorageEngine,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    ):
        self.max_file_size = max_file_size
        self.storage_engine = storage_engine

    def _list_all_immutable_files(self) -> list[ReadableFile]:
        all_filenames = os.listdir(self.storage_engine.directory)
        return [
            ImmutableFile(path=f"{self.storage_engine.directory}/{filename}")
            for filename in all_filenames
            if "active.txt" not in filename
        ]
        # TODO: better handling of active name

    def _merge_files(self, files: list[File]) -> MergedFile:
        """The merging process is as follows:
        1. Read all input files and write only the most recent key for each (i.e. keep in memory to know which has
        already been written)
        2. Whenever the merge file gets bigger than the max file size OR when we are done parsing all files:
            2.a. Flush to disk (i.e. write hashmap to new merged file)
            2.b. Add the hint file next to it
            2.c. Now that the file is ready, we can read from it => update KEY_DIR to reflect the new positions
            2.d. Delete all files that were used in the merging process
            2.e. If there are remaining files to parse, repeat step 2
        """
        # Step 1: Store file info in hashmap
        hashmap = {}
        # Parsing files from oldest to most recent so that we always have the most up-to-date value in the hashmap
        for file in sorted(files):
            for stored_item in file:
                # TODO: this is coupled with fill_from_in_memory_hashmap
                #  - should put both at the same place somehow to decouple !!
                hashmap[stored_item.key] = {
                    "content": stored_item.to_bytes(),
                    "value_size": stored_item.value_size,
                    "value_position_in_row": stored_item.value_position,
                    "timestamp": stored_item.timestamp,
                }

        # Step 2.a: Flush to disk
        # TODO: should be done IF in mem size is bigger than max file size AND when we are done with parsing all files
        merged_file = MergedFile(store_path=self.storage_engine.directory)
        merged_file_key_dir = merged_file.fill_from_in_memory_hashmap(hashmap=hashmap)
        merged_file.close()

        # Step 2.c: Update KEY_DIR
        # TODO: replace __iter__ method on this
        for key, entry in merged_file_key_dir.entries.items():
            # Update in key_dir only those that were searched for in one of the merged files
            # NB: An alternative way to do this would be to compare the timestamps and update the entries that have not
            # been updated more recently. However, timestamps in seconds does not give enough granularity here.
            if self.storage_engine.key_dir.get(key).file_path not in [
                file.path for file in files
            ]:
                continue
            self.storage_engine.key_dir.update(
                key=key,
                file_path=entry.file_path,
                value_position=entry.value_position,
                value_size=entry.value_size,
                timestamp=entry.timestamp,
            )

        # Step 2.d: Delete all files that have been merged together
        for file in files:
            file.discard()

        return merged_file

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def do_merge(self):
        """Merges all files from a given store"""
        all_immutable_files = self._list_all_immutable_files()
        self._merge_files(files=all_immutable_files)

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

from src.io_handling.data_file import (
    MergedDataFile,
    DataFile,
    ImmutableDataFile,
    DataFileItem,
)
from src.io_handling.generic_file import File
from src.io_handling.hint_file import HintFile
from src.storage import Storage


class MergeWorker:
    DEFAULT_FILE_SIZE_THRESHOLD = 1000

    def __init__(
        self,
        storage: Storage,
        file_size_threshold: int = DEFAULT_FILE_SIZE_THRESHOLD,
    ):
        # 'file_size_threshold' is an indicative threshold defining when a new merged file should be created (every time
        # a merged file gets bigger than that threshold, we create a new one).
        # This is not really a max size for the file because most merged files should be slightly bigger than this
        # threshold (the actual max file size will be the sum of this threshold and of the max size for active files).
        self.file_size_threshold = file_size_threshold
        self.storage = storage

    def _get_mergeable_files(self) -> list[DataFile]:
        all_filenames = os.listdir(self.storage.directory)
        return [
            ImmutableDataFile(path=f"{self.storage.directory}/{filename}")
            for filename in all_filenames
            if self.storage.active_data_file.path
            != f"{self.storage.directory}/{filename}"
            and not filename.endswith(".hint")
        ]

    def _create_merge_file(
        self, data_file_items: list[DataFileItem], files: list[File]
    ) -> MergedDataFile:
        """The creation of a new merged file involves the following steps:
        1. Flush rows to disk (i.e. write data_file_items to new merged file)
        2. Add the hint file next to it
        3. Now that the merged file is created, we can read from it => update KEY_DIR to reflect the new positions
        4. Delete all files that were used in the merging process
        """
        # Step 1: Flush to disk
        merged_file = MergedDataFile(store_path=self.storage.directory)
        merged_file_key_dir = merged_file.write(data_file_items=data_file_items)
        merged_file.close()

        # Step 2: Create hint file
        hint_file = HintFile.from_merge_file(merged_file=merged_file)
        hint_file.write(merged_file_key_dir=merged_file_key_dir)

        # Step 3: Update KEY_DIR
        for key, entry in merged_file_key_dir:
            # Update in key_dir only those that were searched for in one of the merged files
            # NB: An alternative way to do this would be to compare the timestamps and update the entries that have not
            # been updated more recently. However, timestamps in seconds does not give enough granularity here.
            if self.storage.key_dir.get(key).file_path not in [
                file.path for file in files
            ]:
                continue

            self.storage.key_dir.update(
                key=key,
                file_path=entry.file_path,
                value_position=entry.value_position,
                value_size=entry.value_size,
                timestamp=entry.timestamp,
            )

        # Step 4: Delete all files that have been merged together
        for file in files:
            file.discard()

        return merged_file

    def _merge_files(self, data_files: list[DataFile]) -> list[MergedDataFile]:
        """The merging process is as follows:
        1. Read input files and record only the most recent value for each key (by storing it in an in-memory hashmap)
        2. Whenever the merge file gets bigger than the max file size OR when we are done parsing all files, we flush
        the hashmap containing all file rows to a new merged file.
        """
        file_rows = {}
        files_being_merged = []
        merged_files = []

        # Parsing files from oldest to most recent so that we always have the most up-to-date value in the hashmap
        data_files.sort()
        while data_files:
            data_file = data_files.pop(0)
            files_being_merged.append(data_file)
            for data_file_item in data_file:
                file_rows[data_file_item.key] = data_file_item
            # Warning: The order of data file items differs from the original one.
            # Not a problem because only one value per key in merged files.
            # An alternative would be to record the rows in a list and the keys and index in a hashmap. Everytime
            # we replace a key, we pop it out of the list and append the new one to the list. This would keep the order.
            data_file_items = list(file_rows.values())
            merged_file_size = sum(
                len(data_file_item.encoded_item) for data_file_item in data_file_items
            )

            # Once the threshold is crossed OR we have processed all files, we flush the rows to the merged file
            if merged_file_size >= self.file_size_threshold or len(data_files) == 0:
                merged_file = self._create_merge_file(
                    data_file_items=data_file_items, files=files_being_merged
                )
                merged_files.append(merged_file)
                files_being_merged = []
                file_rows = {}

        return merged_files

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def do_merge(self):
        """Merges all files from a given store"""
        mergeable_files = self._get_mergeable_files()
        self._merge_files(data_files=mergeable_files)

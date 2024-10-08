from src.storage import Storage


class StorageEngine:
    DEFAULT_DIRECTORY = "./datafiles/default"
    DEFAULT_MAX_FILE_SIZE = 150

    def __init__(
        self,
        directory: str = DEFAULT_DIRECTORY,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    ):
        self.storage = Storage(directory=directory, max_file_size=max_file_size)
        self._boot_up()
        # TODO: add call to merge (=> contains the merge worker)

    def _boot_up(self):
        print("Starting boot up process...")
        print("Building index...")
        self.storage.rebuild_index()
        print("Boot up completed!")

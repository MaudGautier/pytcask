# Pytcask

Pytcask is a toy project aiming at re-implementing [Bitcask](https://github.com/basho/bitcask) from scratch in Python.
This project is for educational purposes only: it aimed at understanding the core principles behind a simple storage
engine.

## Getting started

```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual env
source .venv/bin/activate

# Install the dependencies
python3 -m pip install -r requirements.txt

# Run tests
pytest
```

## Implementation notes

### Overview

Pytcask is a log-structured key-value store mimicking the behavior of Riak's default storage
engine: [Bitcask](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html).

[//]: # (It is optimized for write-heavy applications &#40;inserting/updating a record is done in constant time: `o&#40;1&#41;`&#41; where the)

[//]: # (number of distinct keys is relatively small &#40;they must all hold in memory&#41;.)

### Main operations

**Writes:**
Because Pytcask is log-structured, key-value records are appended sequentially to data files.
Therefore, inserting and updating records is always done in constant time (`o(1)`).

**Reads:**
During a key lookup, the key is searched in the `KeyDir` (the in-memory hash table, see below) which contains
information about the record location on disk. The corresponding datafile is then read at the correct offset.
Thus, reading requires only one single disk seek.

**Deletions:**
Deleting a record cannot be done by removing it from the log file since it is append-only.
Instead, a deleted record is marked by a tombstone (= a special record that indicates that this key has been deleted).
The tombstone will be used to discard all records corresponding to that key during the merge process.
In addition, the key is removed from the `KeyDir` to indicate that the key has no associated value.

**Boot-up process:**
Since the `KeyDir` is stored in memory, it will be lost if the server crashes (or even if it stops gracefully).
Upon restart, the `KeyDir` must be rebuilt from the records stored on disk. One way to do it would be to read all data
files and build the `KeyDir` from there. To be more efficient, a hint file is associated to each data file: it contains
only the information included in the `KeyDir` so that it can be built faster.

**Merge operations:**
Since records are only ever appended to data files, anytime a key is assigned a new value, all previous values are never
read anymore.
To reclaim disk space corresponding to all those obsolete records, a merge process runs in the background: it reads all
data files in descending order of creation (from most recent to oldest) and stores the metadata (data file and offset)
of the most recently written record for each key in an in-memory hash map. This hash map will then override the
current `KeyDir`.
In addition, new merged (and compressed) files are created, and old data files are discarded.

**Characteristics and limitations:**
Writes are made sequentially, and thus in constant time (`o(1)`).
Reads are also made in constant time, requiring one lookup in the `KeyDir` and one disk seek in the file indicated by
the `KeyDir` lookup.
The main limitation is that all keys must hold in memory (inside the `KeyDir`). Therefore, this storage engine is
adapted for applications that are both read and write-heavy, as long as the number of distinct keys remains relatively
small.

### Main components

- **KeyDir**: Hash table kept in memory that records each key in the dataset and maps them with their offset in data
  files.
- **DataFile**: Contains all records, i.e. pairs of key-value + metadata: timestamp. Serialization and deserialization
  of records occur upon insertion into and retrieval from data files.
- **HintFile**: There is one per data file. It contains all the keys from its associated data file and the
  meta-information (offset of the record within the data file). It is used to allow performant bootups.
- **MergeWorker**: Handles merge operations in the background to reclaim disk space by compacting and merging data files
  and discarding obsolete records.
- **Storage**: Exposes all commands (`get`, `insert`, `delete`, ...).

## References

To build the project, I used the following:

- The reference paper explaining the main concepts of Bitcask: https://riak.com/assets/bitcask-intro.pdf
- The source code of Bitcask (in Erlang): https://github.com/basho/bitcask

# TODO:

- [x] Append only to one single file
- [x] Add an interface to add/search key
- [x] When the file meets a certain size threshold => open a new active one
  -> requires something pointing to the active file and the rest of immutable files
- [x] Create a hash index = keydir
- [x] Add compaction/merging process on the list of immutable files
- [x] Add hint file next to each compacted/merged file
- [x] Write a new key-value pair: Needs to be an atomic operation for 2 things: append to the file + add to the keydir
- [x] Open the active file only once. When it is closed (either because it is full or intentionally because of a crash),
  it is never reopened again: it is considered immutable.
- [ ] Handle key deletion with a tombstone
- [ ] When reading a value, the correctness of the value retrieved is checked against the CRC
- [x] For bootup: read hint files
- [ ] Make operation atomic to update keydir / create hint file

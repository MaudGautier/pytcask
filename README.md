# Custom implementation of bitcask

## Use the project

```shell
# Activate the virtual env
source .venv/bin/activate

# Install the dependencies
python3 -m pip install -r requirements.txt
```

## Notes on how to proceed

Reference papers:
- Broad overview of the concepts: https://riak.com/assets/bitcask-intro.pdf
- Very good article with many useful details: https://arpitbhayani.me/blogs/bitcask/
- Bitcask source code: https://github.com/basho/bitcask
- 

Original plan:
- [x] Append only to one single file
- [ ] Add an interface to add/search key
- [ ] When the file meets a certain size threshold => open a new active one 
  -> requires something pointing to the active file and the rest of immutable files 
- [x] Create a hash index = keydir 
- [ ] Add compaction/merging process on the list of immutable files
- [ ] Add hint file next to each compacted/merged file
- [ ] Write a new key-value pair: Needs to be an atomic operation for 2 things: append to the file + add to the keydir
- [ ] Open the active file only once. When it is closed (either because it is full or intentionally because of a crash), it is never reopened again: it is considered immutable.
- [ ] Handle key deletion with a tombstone
- [ ] When reading a value, the correctness of the value retrieved is checked against the CRC
- [ ] For bootup: read hint files

Unclear thoughts:
- Command line arguments to go through this ?
- How to handle which file active, which folder of inactive ones
- How to know which are merged (timestamp of last added key in the file name??)
- How to ensure that I don't reopen a file? (only create it or read from it)
- 

Things I need to learn about:
- The OS's read-ahead cache (which apparently, can server the data without going to disk when requesting the value for a particular key).
- What an Erlang process is

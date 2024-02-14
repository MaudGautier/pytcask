# Custom implementation of bitcask

Reference papers:
- Broad overview of the concepts: https://riak.com/assets/bitcask-intro.pdf
- 

Original plan:
- [ ] Append only to one single file
- [ ] Add an interface to add/search key
- [ ] When the file meets a certain size threshold => open a new active one 
  -> requires something pointing to the active file and the rest of immutable files 
- [ ] Create a hash index = keydir 
- [ ] Add compaction/merging process on the list of immutable files
- [ ] Add hint file next to each compacted/merged file

Unclear thoughts:
- Command line arguments to go through this ?
- How to handle which file active, which folder of inactive ones
- How to know which are merged (timestamp of last added key in the file name??)
- 


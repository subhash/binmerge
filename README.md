# Binary Merge

A solution for the Merge problem described [here](https://github.com/faunadb/exercises/blob/master/merge.md)

## Usage

```bash
lein test binmerge.core-test
```
tests core functionality

```bash
lein test binmerge.perf-test
```
tests memory profile of execution

## Notes

* Function logic and algorithm are described as part of code documentation in `src/binmerge/core.clj`. Sections `Iterators` and `Merge functions` are most relevant
* Two kinds of traversals are possible - using the sequence model or working directly with byte arrays - the former being more expressive and the latter more efficient. `merge-bin` uses the former strategy assuming merge is a background operation and can afford more cycles than `find` which uses the more efficient latter strategy
* Resolution Strategy: When objects conflict, we merge their attributes. When attributes conflict by key, we simply take the first and since files are sorted by modified time, that can be assumed to be the latest update
* Incremental processing: tested as part of `binmerge.perf-test/test-object-iteration`
* Extra credit: The object reader function uses a simple iteration using native byte arrays. It is considered to be performant because: 
  * It reads only as much as is necessary
  * Employs tail-recursion to prevent stack accumulation
* Extra credit: The merge function iterates over attributes without having the load the entire object into memory. For the purpose of updating the attr no count, it keeps track of the tuple [position attr-no] and updates the correct attr no after the iteration

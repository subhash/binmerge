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
* When objects conflict, we merge their attributes. When attributes conflict by key, we simply take the first assuming that to be the latest version
* Incremental processing is tested as part of `binmerge.perf-test/memory-footprint-test`
* The object finder uses a simple iteration using native byte arrays. It is considered to be performant because: 
  * It reads only as much as is necessary
  * Employs tail-recursion to prevent stack accumulation
* Though the merge function iterates over attributes without having the load the entire object into memory, it has a necessity to accumulate the resultant attributes before writing them to the output stream because the count of attributes precedes the attributes themselves in the format

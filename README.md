# b-table

A persistent database table based on [b-tree](https://github.com/haydnv/b-tree), with support for multiple indices.

`TableLock::create` requires empty delegated storage and awaits admission of each
index root in order. Interrupted creation can leave an incomplete destination.
`TableLock::load` requires the primary index and every schema-declared auxiliary
index, including their BTree roots; missing indexes are errors. Synchronize initial canonical storage
before relying on restart loading.

Run `cargo test --all-targets --all-features` to include the native integration tests.

Callers supply a schema implementation, collator, and delegated directory.
See the [executable example](examples/example.rs) for construction, mutation,
selection, and streaming with concrete schema and file adapters.

## Filesystem codecs

Table delegates block mutation and synchronization to its native index storage.
`sync()` is buffered; `sync_all()` is an explicit durability barrier, not atomic
multi-index publication. The caller owns recovery from interrupted updates.
`validate()` checks native trees and auxiliary consistency against primary rows.
`copy_into()` copies native indexes into empty delegated storage.

Index schemas validate complete rows and nonempty typed routing prefixes.
Recreate storage containing old copied-minimum internal nodes; loading does not
migrate or repair it.

File entries implement `freqfs::FileLoad` and `FileSave`. Loads reconstruct the
same entry type that saves write; typed access validates the resulting entry via
`AsType`. Adapters must preserve payload identity across persistence rather than
reinterpret bytes as whichever type a reader requests.

The `stream` feature supplies destream implementations without selecting a byte
codec. Applications implement `FileLoad`/`FileSave` for their file entry type
using their chosen codec. The examples choose TBON explicitly.

`Table::upsert_sorted` consumes fallible key/value pairs ordered by the complete
primary-index row, beginning at or beyond its current maximum. It validates rows,
delegates ordered insertion to the native tree, and maintains auxiliary indexes
through ordinary insertion. Errors may leave partial changes, as with other
native writes; callers own unpublished construction and recovery.

`Table::upsert` replaces an existing primary-key row and updates changed auxiliary
entries. It returns `true` for insertion and `false` for replacement. Semantic
equality does not suppress publication, including in auxiliary indexes.
`upsert_sorted` remains ordered insertion for construction; it does not replace
existing primary-key values. Primary replacement preserves row order
and topology through native B-tree replacement. Point access resolves semantic
primary-key equality within the collation group; multiple matching rows are an
ambiguity error before mutation.

Auxiliary differences are applied first; even a primary replacement rejection
can leave partial index changes. The caller owns recovery.

`Table::try_update` resolves a primary-key row once and lends it to a synchronous,
fallible callback. The callback returns replacement value columns, or `None` to
delete; the key remains fixed. Table validates the result and owns index maintenance.
Callback and validation errors are nonmutating; publication retains the partial-mutation
contract above. The callback must not reacquire the table's write owner.

Row mutations overlap independent auxiliary-index operations within the acquired
Table write owner. Semantically equal entries use native replacement; changed
entries are deleted before insertion. Primary publication waits for all auxiliary
operations. Ordered ingestion retains row order while overlapping auxiliary work
within each row. Errors cancel unfinished siblings and may leave partial auxiliary changes; error/index completion order is
not guaranteed. Index-lock acquisition remains ordered. Concurrent operations may
retain more cache capacity, with admission delegated to freqfs.

## Timing entrypoint

Run `benchmark_index_mutations` separately from correctness tests. The shared
comparison runner sets both Rayon and Tokio worker counts. For a direct run:

```sh
TOKIO_WORKER_THREADS=1 RAYON_NUM_THREADS=1 cargo test --release --features stream --test native benchmark::benchmark_index_mutations -- --exact --ignored --nocapture
```

Use `4` for both variables to select four workers. Without an explicit
`TOKIO_WORKER_THREADS`, Tokio uses its ordinary multithreaded runtime default.

The executable example shares its schema and file adapters from `examples/support/`.
Regression tests live in modules under `tests/native/`, compiled as one `native`
integration target; test-only observations and directory fixtures live in `tests/common/`.

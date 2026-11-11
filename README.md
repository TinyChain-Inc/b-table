# b-table

A persistent database table based on
[b-tree](https://github.com/TinyChain-Inc/b-tree), with support for multiple indices.

`TableLock::create` requires empty delegated storage and awaits admission of each
index root in order. Interrupted creation can leave an incomplete destination.
`TableLock::load` requires the primary index and every schema-declared auxiliary
index, including their BTree roots; missing indexes are errors. Synchronize initial
canonical storage before relying on restart loading.

Auxiliary index names must be unique and cannot use the reserved name `primary`.
Creation and loading reject invalid names before accessing storage.

Run `cargo test --all-targets --all-features` to include the native integration tests.

Callers supply a schema implementation, collator, and delegated directory.
See the [executable example](examples/example.rs) for construction, mutation,
selection, and streaming with concrete schema and file adapters. Covering row
scans select columns while borrowing native leaves, so unselected payloads are
not cloned into intermediate rows.
Projection preserves requested order, including repeated columns: each occurrence
contains the selected value. Occurrence metadata is prepared once per stream;
unique fields and final occurrences move out of the owned row, while additional
occurrences clone. Empty and missing-column selections remain errors.
Internally, count and emptiness project zero fields through the same terminal
scan. They retain row multiplicity, validation, buffering, and error order without
cloning values. These internal zero-field rows do not change public selection.
Emptiness and count select the same supporting index as ordinary row scans.

Column-range containment includes inclusive endpoints and every empty range.
Range intersection returns `None` if any constraint is empty or reversed,
including constraints appearing in only one operand. Comparisons use the schema's
collator, whose equivalence need not imply value equality.

Table reads return owned rows and streams. Scoped native borrowing belongs to
BTree; Table exposes one query-consumption implementation. The former
`get_row_with` and `visit_rows` APIs have been removed without aliases.

## Filesystem codecs

Table delegates block mutation and synchronization to its native index storage.
`sync()` is buffered; `sync_all()` is an explicit durability barrier, not atomic
multi-index publication. The caller owns recovery from interrupted updates.
`validate()` checks native trees, semantic primary-key uniqueness, and auxiliary
consistency against primary rows. It retains one previous key prefix and resolves
distinct collation-tied prefixes through the primary index to detect separated
duplicates without collecting a collation group.
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

`Table::upsert` replaces an existing primary-key row and updates changed auxiliary
entries. It returns `true` for insertion and `false` for replacement. Semantic
equality does not suppress publication, including in auxiliary indexes.
Primary replacement preserves row order
and topology through native B-tree replacement. Point access resolves semantic
primary-key equality within the collation group; multiple matching rows are an
ambiguity error before mutation.

Auxiliary differences are applied first; even a primary replacement rejection
can leave partial index changes. The caller owns recovery.

First-row reads and retained row streams share selected-index execution. Scoped
reads borrow only the selected and primary indexes instead of rebuilding the
index map. Index consistency validation prepares column positions once, copies
only auxiliary-key fields, and borrows rows when counting auxiliary entries.
Its ordered scans and stored-data checks remain the same.

`upsert` and deletion are the mutation boundary. Callback mutation and specialized
ordered Table ingestion have been removed; callers construct rows and apply them
through ordinary upsert. BTree retains its native ordered construction API.

Row mutations overlap independent auxiliary-index operations within the acquired
Table write owner. Semantically equal entries use native replacement; changed
entries are deleted before insertion. Primary publication waits for all auxiliary
operations. Errors cancel unfinished siblings and may leave partial auxiliary
changes; error/index completion order is not guaranteed. Index-lock acquisition
remains ordered. Concurrent operations may retain more cache capacity, with
admission delegated to freqfs.

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

First-row lookup, emptiness, and range deletion execute the same selected-index scan.
Deletion releases traversal access before applying each selected row. Repeated
output columns preserve their values and requested order. Scoped native streams
may borrow storage ownership; existing owned streams remain available. `first`
uses the same cloneable, thread-safe collator requirements as row traversal.
Noncovering scans resolve primary keys by exact semantic identity, preserving
distinct keys which collate equally.

Multicolumn ranges support query planning and intersection, not the former
`OverlapsRange` implementation. Column constraints retain their single-column
comparisons; no replacement multidimensional ordering is provided.

Queries use one supporting index: the primary index first, then auxiliary indexes
in declaration order. Constraints must be leading equalities followed by at most
one range, and requested ordering must match a literal index-column prefix.
Unrestricted scans remain streamed. Covering projections read that index directly;
noncovering results resolve exact primary keys. Unsupported combinations fail
before traversal or deletion: use a matching composite index or rewrite the
predicate into the supported equality-prefix/range form. No sorting, filtering,
repeated index passes, or automatic materialization is performed.

The timing entrypoint retains lookup, scan, replacement and deletion controls.
Its former ordered-insert series is retired; retained historical measurements
continue describing the source that produced them and are not relabeled.

Mutations use row upsert/removal, range deletion, and truncation. The former
cross-table `merge` and `delete_all` shortcuts are removed; auxiliary indexes are
maintained through the row mutation path rather than independent bulk index edits.

# b-table

A persistent database table based on [b-tree](https://github.com/haydnv/b-tree), with support for multiple indices.

`TableLock::create` requires empty delegated storage and awaits admission of each
index root in order. Interrupted creation can leave an incomplete destination.
`TableLock::load` requires the primary index and every schema-declared auxiliary
index, including their BTree roots; missing indexes are errors. Synchronize initial canonical storage
before relying on restart loading.

Run `cargo test --all-targets --all-features` to include the strict-load example test.

Example usage:
```rust
use b_table::{Collator, Schema, TableLock};

enum ColumnValue {
    U64(u64),
    Str(String),
}

# ...

// note: b-table provides a Schema trait but not a struct which implements it
let schema = Schema::new(
    ["zero", "one", "two", "value"],
    [
        ("index_one", ["one", "zero", "two"]),
        ("index_two", ["two", "zero", "one"]),
    ]
);

let key: Vec<ColumnValue> = vec![0.into(), 0.into(), 0.into()];
let value: Vec<ColumnValue> = vec!["value".into()];

let table = TableLock::create(schema, Collator::new(), dir).await?;

{
    let mut table = table.write().await; // or table.try_write()?
    table.upsert(key.clone(), value.clone()).await?;
    assert_eq!(table.get_value(key.clone())).await?, Ok(Some(value.clone()));
}

let mut expected_row = key;
expected_row.extend(value);

{
    let order = &["two", "one", "zero"];
    let range = [("one", 0)].into_iter().collect();
    let table = table.read().await; // or table.try_read()?
    let mut rows = table.rows(order, range, Some(&["value"]))?;
    assert_eq!(rows.try_next().await, Ok(Some(expected_row)));
}
```

## Filesystem codecs

Table delegates block mutation and synchronization to its native index storage.
`sync()` is buffered; `sync_all()` is an explicit durability barrier, not atomic
multi-index publication. The caller owns recovery from interrupted updates.
`validate()` checks native trees and auxiliary consistency against primary rows.
`copy_into()` copies native indexes into empty delegated storage.

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
entries. It returns `true` for insertion and `false` for replacement or an
unchanged row. `upsert_sorted` remains ordered insertion for construction; it does
not replace existing primary-key values. Primary replacement preserves row order
and topology through native B-tree replacement. Rows sharing a primary prefix
can therefore cause ordering or collator-ambiguity errors. Auxiliary differences
are applied first; even a primary replacement rejection can leave partial index
changes. The caller owns recovery.

`Table::try_update` resolves a primary-key row once and lends it to a synchronous,
fallible callback. The callback returns replacement value columns, or `None` to
delete; the key remains fixed. Table validates the result and owns index maintenance.
Callback and validation errors are nonmutating; publication retains the partial-mutation
contract above. The callback must not reacquire the table's write owner.

Row mutations overlap independent auxiliary-index operations within the acquired
Table write owner. Each index deletes its old entry before inserting the new one;
primary publication waits for all auxiliary operations. Ordered ingestion retains
row order while overlapping auxiliary work within each row. Errors cancel unfinished
siblings and may leave partial auxiliary changes; error/index completion order is
not guaranteed. Index-lock acquisition remains ordered. Concurrent operations may
retain more cache capacity, with admission delegated to freqfs.

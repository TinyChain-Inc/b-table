# b-table
A persistent database table based on [b-tree](https://github.com/haydnv/b-tree), with support for multiple indices.

`TableLock::create` requires empty delegated storage. `TableLock::load` requires
the primary index and every schema-declared auxiliary index, including their
BTree roots; missing indexes are errors. Synchronize initial canonical storage
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

let table = TableLock::create(schema, Collator::new(), dir)?;

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

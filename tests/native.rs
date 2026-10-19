use std::cmp::Ordering;
use std::io;
use std::ops::Bound;

use freqfs::Cache;
use futures::{FutureExt, TryStreamExt};
use get_size::GetSize;
use number_general::NumberCollator;

use self::adapters::{BLOCK_SIZE, Collator, File, IndexSchema, TableSchema, Value};
use self::directory::TestDir;
use self::observations::INDEXED_CLONES;
use b_table::collate::Collate;
use b_table::{Node, Range, TableLock};

#[path = "../examples/support/adapters.rs"]
mod adapters;
#[path = "common/directory.rs"]
mod directory;
#[path = "common/observations.rs"]
mod observations;

#[path = "native/benchmark.rs"]
mod benchmark;
#[path = "native/concurrency.rs"]
mod concurrency;
#[path = "native/mutation.rs"]
mod mutation;
#[path = "native/storage.rs"]
mod storage;

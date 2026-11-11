//! Index selection owns query support; no traversal is required to reject a query.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestIndex(Vec<&'static str>);

impl BTreeSchema for TestIndex {
    type Error = io::Error;
    type Value = u64;

    fn block_size(&self) -> usize {
        4_096
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn order(&self) -> usize {
        10
    }

    fn validate_prefix(&self, prefix: &[u64]) -> io::Result<()> {
        if prefix.is_empty() || prefix.len() > self.len() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid separator width",
            ))
        } else {
            Ok(())
        }
    }

    fn validate_key(&self, key: Vec<u64>) -> io::Result<Vec<u64>> {
        if key.len() == self.len() {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid row width",
            ))
        }
    }
}

impl IndexSchema for TestIndex {
    type Id = &'static str;

    fn columns(&self) -> &[Self::Id] {
        &self.0
    }
}

#[derive(Debug, Eq, PartialEq)]
struct TestTable {
    primary: TestIndex,
    auxiliary: Vec<(String, TestIndex)>,
}

impl Schema for TestTable {
    type Id = &'static str;
    type Error = io::Error;
    type Value = u64;
    type Index = TestIndex;

    fn key(&self) -> &[Self::Id] {
        &self.primary.0[..1]
    }

    fn values(&self) -> &[Self::Id] {
        &self.primary.0[1..]
    }

    fn primary(&self) -> &TestIndex {
        &self.primary
    }

    fn auxiliary(&self) -> &[(String, TestIndex)] {
        &self.auxiliary
    }

    fn validate_key(&self, key: Vec<u64>) -> io::Result<Vec<u64>> {
        if key.len() == self.key().len() {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid key width",
            ))
        }
    }

    fn validate_values(&self, values: Vec<u64>) -> io::Result<Vec<u64>> {
        if values.len() == self.values().len() {
            Ok(values)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid value width",
            ))
        }
    }
}

fn schema() -> TableSchema<TestTable> {
    TestTable {
        primary: TestIndex(vec!["id", "group", "score", "value"]),
        auxiliary: vec![
            ("by_group".into(), TestIndex(vec!["group", "id", "score"])),
            ("group_tie".into(), TestIndex(vec!["group", "id", "score"])),
            ("by_score".into(), TestIndex(vec!["score", "id", "group"])),
        ],
    }
    .into()
}

#[test]
fn primary_supports_unrestricted_equality_and_one_trailing_range() {
    let schema = schema();

    for (range, order) in [
        (HashMap::new(), vec![]),
        (HashMap::new(), vec!["id", "group"]),
        (HashMap::from([("id", ColumnRange::Eq(1))]), vec![]),
        (HashMap::from([("id", (1..4).into())]), vec!["id"]),
        (
            HashMap::from([
                ("group", 2.into()),
                ("id", 1.into()),
                ("score", (3..5).into()),
            ]),
            vec!["id", "group", "score"],
        ),
    ] {
        assert_eq!(
            schema.query_index(&range, &order).unwrap(),
            IndexId::Primary
        );
    }
}

#[test]
fn auxiliary_supports_the_complete_range_and_literal_order_prefix() {
    let schema = schema();

    for (range, order, expected) in [
        (HashMap::from([("group", 2.into())]), vec![], "by_group"),
        (
            HashMap::from([("group", 2.into()), ("id", (1..4).into())]),
            vec!["group", "id"],
            "by_group",
        ),
        (HashMap::new(), vec!["group"], "by_group"),
        (
            HashMap::from([("score", (2..5).into())]),
            vec!["score", "id"],
            "by_score",
        ),
    ] {
        assert_eq!(
            schema.query_index(&range, &order).unwrap(),
            IndexId::Auxiliary(expected)
        );
    }
}

#[test]
fn unsupported_ranges_and_orders_reject_without_a_plan() {
    let schema = schema();

    for (range, order) in [
        (
            HashMap::from([("id", (1..4).into()), ("group", (1..4).into())]),
            vec![],
        ),
        (
            HashMap::from([("id", (1..4).into()), ("value", 2.into())]),
            vec![],
        ),
        (HashMap::from([("value", 2.into())]), vec![]),
        (HashMap::from([("missing", 2.into())]), vec![]),
        (HashMap::new(), vec!["missing"]),
        (HashMap::from([("group", 2.into())]), vec!["id"]),
    ] {
        let error = schema.query_index(&range, &order).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }
}

#[test]
fn selection_prefers_primary_then_auxiliary_declaration_order() {
    let mut schema = schema();

    assert_eq!(
        schema.query_index(&HashMap::new(), &[]).unwrap(),
        IndexId::Primary
    );

    let range = HashMap::from([("group", ColumnRange::Eq(2))]);
    assert_eq!(
        schema.query_index(&range, &[]).unwrap(),
        IndexId::Auxiliary("by_group")
    );

    schema.inner.auxiliary.swap(0, 1);
    assert_eq!(
        schema.query_index(&range, &[]).unwrap(),
        IndexId::Auxiliary("group_tie")
    );
}

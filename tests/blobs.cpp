#include <rstd/test/gtest.hpp>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;

TEST(SqliteBlob, PersistsExactBytesAcrossFileReopen) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = rstd::path::PathBuf::from(directory.path()).join("blob.sqlite"_str);
    auto input     = Vec<u8>::with_capacity(usize(65536));
    for (unsigned i = 0; i < 65536; ++i) input.push(u8(i % 256));
    {
        auto db = sqlito::Connection::open(path.as_path().to_str().unwrap()).unwrap();
        {
            auto create =
                db.prepare("CREATE TABLE data(payload BLOB NOT NULL) STRICT"_str).unwrap();
            create.step().unwrap();
        }
        auto insert = db.prepare("INSERT INTO data VALUES(?1)"_str).unwrap();
        insert.bind_blob(usize(1), input.as_slice()).unwrap();
        insert.step().unwrap();
    }
    auto db     = sqlito::Connection::open(path.as_path().to_str().unwrap()).unwrap();
    auto select = db.prepare("SELECT payload FROM data"_str).unwrap();
    select.step().unwrap();
    auto output = select.blob(usize()).unwrap().unwrap();
    ASSERT_EQ(output.len(), input.len());
    for (usize i {}; i < input.len(); ++i) ASSERT_EQ(output[i].get(), input[i].get());
}

TEST(SqliteBlob, CopiesBindingAndReturnsOwnedBinaryAcrossResetAndFinalize) {
    Vec<u8> saved;
    {
        auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
        auto statement = db.prepare("SELECT ?1"_str).unwrap();
        {
            auto input = array<u8, 4> { u8(), u8(255), u8(128), u8(65) };
            statement.bind_blob(usize(1), input.as_slice()).unwrap();
            input[usize(1)] = u8(7);
        }
        EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Row);
        saved = statement.blob(usize()).unwrap().unwrap();
        EXPECT_EQ(saved.len(), usize(4));
        EXPECT_EQ(saved[usize(1)], u8(255));
        EXPECT_EQ(statement.text(usize()).unwrap_err().kind(), sqlito::ErrorKind::Type);
        EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Done);
        statement.reset().unwrap();
        EXPECT_EQ(statement.step().unwrap_err().kind(), sqlito::ErrorKind::Unbound);
    }
    EXPECT_EQ(saved[usize()], u8());
    EXPECT_EQ(saved[usize(2)], u8(128));
    EXPECT_EQ(saved[usize(3)], u8(65));
}

TEST(SqliteBlob, DistinguishesEmptyNullAndOtherStorageTypes) {
    auto    db        = sqlito::Connection::open(":memory:"_str).unwrap();
    auto    statement = db.prepare("SELECT ?1,NULL,'text',7,1.5,typeof(?1)"_str).unwrap();
    Vec<u8> empty;
    statement.bind_blob(usize(1), empty.as_slice()).unwrap();
    statement.step().unwrap();
    auto value = statement.blob(usize()).unwrap();
    ASSERT_TRUE(value.is_some());
    EXPECT_TRUE(value->is_empty());
    EXPECT_TRUE(statement.blob(usize(1)).unwrap().is_none());
    EXPECT_EQ(statement.text(usize(5)).unwrap()->as_str(), "blob"_str);
    for (usize column(2); column < usize(5); ++column)
        EXPECT_EQ(statement.blob(column).unwrap_err().kind(), sqlito::ErrorKind::Type);
}

TEST(SqliteBlob, EnforcesStateIndicesMovesAndTransactionLifetime) {
    auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
    auto tx        = db.begin(sqlito::TransactionMode::Immediate).unwrap();
    auto statement = tx.prepare("SELECT ?1"_str).unwrap();
    auto bytes     = "binary"_str.as_bytes();
    EXPECT_EQ(statement.blob(usize()).unwrap_err().kind(), sqlito::ErrorKind::State);
    EXPECT_EQ(statement.bind_blob(usize(), bytes).unwrap_err().kind(), sqlito::ErrorKind::Range);
    EXPECT_EQ(statement.bind_blob(usize(2), bytes).unwrap_err().kind(), sqlito::ErrorKind::Range);
    statement.bind_blob(usize(1), bytes).unwrap();
    auto moved = rstd::move(statement);
    EXPECT_EQ(statement.bind_blob(usize(1), bytes).unwrap_err().kind(), sqlito::ErrorKind::State);
    moved.step().unwrap();
    EXPECT_EQ(moved.blob(usize(1)).unwrap_err().kind(), sqlito::ErrorKind::Range);
    EXPECT_EQ(moved.bind_blob(usize(1), bytes).unwrap_err().kind(), sqlito::ErrorKind::State);
    auto saved = moved.blob(usize()).unwrap().unwrap();
    tx.rollback().unwrap();
    EXPECT_EQ(moved.blob(usize()).unwrap_err().kind(), sqlito::ErrorKind::State);
    EXPECT_EQ(saved.len(), bytes.len());
}

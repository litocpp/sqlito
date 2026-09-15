#include <rstd/test/gtest.hpp>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;

template<class Session>
void execute(Session& db, ref<str> sql) {
    auto stmt = db.prepare(sql).unwrap();
    EXPECT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
}
TEST(Transactions, OwnershipAndExpiredStatements) {
    auto db   = sqlito::Connection::open(":memory:"_str).unwrap();
    auto base = db.prepare("SELECT ?"_str).unwrap();
    auto tx   = db.begin().unwrap();
    EXPECT_TRUE(db.prepare("SELECT 1"_str).is_err());
    EXPECT_TRUE(db.begin().is_err());
    EXPECT_TRUE(db.close().is_err());
    EXPECT_TRUE(base.bind_null(usize(1)).is_err());
    EXPECT_TRUE(base.reset().is_err());
    auto old = tx.prepare("SELECT ?"_str).unwrap();
    ASSERT_TRUE(tx.commit().is_ok());
    EXPECT_TRUE(tx.prepare("SELECT 1"_str).is_err());
    EXPECT_TRUE(tx.commit().is_err());
    ASSERT_TRUE(base.bind_null(usize(1)).is_ok());
    auto next = db.begin().unwrap();
    EXPECT_TRUE(old.bind_null(usize(1)).is_err());
    EXPECT_TRUE(old.step().is_err());
    EXPECT_TRUE(old.reset().is_err());
    ASSERT_TRUE(next.rollback().is_ok());
    ASSERT_TRUE(base.step().is_ok());
    EXPECT_TRUE(db.begin().is_err());
}
TEST(Transactions, RejectsRawControlAndRecoversFailedCommit) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    EXPECT_TRUE(db.prepare("BEGIN"_str).is_err());
    EXPECT_TRUE(db.prepare("SAVEPOINT x"_str).is_err());
    execute(db, "CREATE TABLE notes(id INTEGER)"_str);
    auto tx = db.begin(sqlito::TransactionMode::Immediate).unwrap();
    EXPECT_TRUE(tx.prepare("COMMIT"_str).is_err());
    EXPECT_TRUE(tx.prepare("ROLLBACK"_str).is_err());
    auto write = tx.prepare("INSERT INTO notes VALUES (1) RETURNING id"_str).unwrap();
    ASSERT_EQ(write.step().unwrap(), sqlito::Step::Row);
    EXPECT_TRUE(tx.commit().is_err());
    EXPECT_TRUE(db.begin().is_err());
    ASSERT_TRUE(write.reset().is_ok());
    ASSERT_TRUE(tx.commit().is_ok());
    EXPECT_TRUE(write.integer(usize()).is_err());
    auto next = db.begin().unwrap();
    ASSERT_TRUE(next.rollback().is_ok());
}
TEST(Transactions, AutomaticRollbackInvalidatesOwner) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    execute(db, "CREATE TABLE notes(id INTEGER PRIMARY KEY)"_str);
    auto tx = db.begin().unwrap();
    execute(tx, "INSERT INTO notes VALUES (1)"_str);
    auto duplicate = tx.prepare("INSERT OR ROLLBACK INTO notes VALUES (1)"_str).unwrap();
    ASSERT_TRUE(duplicate.step().is_err());
    EXPECT_TRUE(tx.commit().is_err());
    EXPECT_TRUE(duplicate.reset().is_err());
    auto next  = db.begin().unwrap();
    auto count = next.prepare("SELECT count(*) FROM notes"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
    ASSERT_TRUE(next.rollback().is_ok());
}
TEST(Transactions, TransactionOutlivesConnection) {
    auto tx = [] {
        auto db = sqlito::Connection::open(":memory:"_str).unwrap();
        return db.begin().unwrap();
    }();
    execute(tx, "CREATE TABLE notes(id INTEGER)"_str);
    ASSERT_TRUE(tx.commit().is_ok());
}
TEST(Transactions, MoveAssignmentRollsBackPreviousTransaction) {
    auto left  = sqlito::Connection::open(":memory:"_str).unwrap();
    auto right = sqlito::Connection::open(":memory:"_str).unwrap();
    execute(left, "CREATE TABLE notes(id INTEGER)"_str);
    auto first = left.begin().unwrap();
    execute(first, "INSERT INTO notes VALUES (1)"_str);
    auto second = right.begin().unwrap();
    first       = rstd::move(second);
    EXPECT_TRUE(second.commit().is_err());
    ASSERT_TRUE(first.commit().is_ok());
    auto count = left.prepare("SELECT count(*) FROM notes"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
}
TEST(Transactions, FailedCommitCanRollbackWithLiveStatement) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    execute(db, "CREATE TABLE notes(id INTEGER)"_str);
    {
        auto tx    = db.begin().unwrap();
        auto write = tx.prepare("INSERT INTO notes VALUES (1) RETURNING id"_str).unwrap();
        ASSERT_EQ(write.step().unwrap(), sqlito::Step::Row);
        EXPECT_TRUE(tx.commit().is_err());
        ASSERT_TRUE(tx.rollback().is_ok());
        EXPECT_TRUE(write.step().is_err());
        EXPECT_TRUE(write.integer(usize()).is_err());
    }
    auto count = db.prepare("SELECT count(*) FROM notes"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
}
TEST(Transactions, FileReopenPreservesOnlyCommittedWrites) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = rstd::path::PathBuf::from(directory.path());
    path.push(rstd::path::PathBuf::from("notes.sqlite"_str).as_path());
    auto name = path.as_path().to_str().unwrap();
    {
        auto db = sqlito::Connection::open(name).unwrap();
        execute(db, "CREATE TABLE notes(id INTEGER)"_str);
        {
            auto tx = db.begin(sqlito::TransactionMode::Exclusive).unwrap();
            execute(tx, "INSERT INTO notes VALUES (1)"_str);
            ASSERT_TRUE(tx.commit().is_ok());
        }
        {
            auto tx = db.begin().unwrap();
            execute(tx, "INSERT INTO notes VALUES (2)"_str);
            ASSERT_TRUE(tx.rollback().is_ok());
        }
        {
            auto tx = db.begin().unwrap();
            execute(tx, "INSERT INTO notes VALUES (3)"_str);
        }
        ASSERT_TRUE(db.close().is_ok());
    }
    auto db     = sqlito::Connection::open(name).unwrap();
    auto result = db.prepare("SELECT count(*), sum(id) FROM notes"_str).unwrap();
    ASSERT_EQ(result.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*result.integer(usize()).unwrap(), i64(1));
    EXPECT_EQ(*result.integer(usize(1)).unwrap(), i64(1));
}

#include <rstd/test/gtest.hpp>
import sqlito;
using namespace rstd::prelude;
using namespace rstd::literals;

TEST(Sqlite, IdleTracksOwnedStatementsAcrossMovesAndFtsCaches) {
    auto first  = sqlito::Connection::open(":memory:"_str).unwrap();
    auto second = sqlito::Connection::open(":memory:"_str).unwrap();
    EXPECT_TRUE(first.idle());
    {
        auto a = first.prepare("SELECT 1"_str).unwrap();
        auto b = second.prepare("SELECT 2"_str).unwrap();
        EXPECT_FALSE(first.idle());
        EXPECT_FALSE(second.idle());
        b = rstd::move(a);
        EXPECT_FALSE(first.idle());
        EXPECT_TRUE(second.idle());
        auto c = rstd::move(b);
        EXPECT_FALSE(first.idle());
        EXPECT_EQ(c.step().unwrap(), sqlito::Step::Row);
    }
    EXPECT_TRUE(first.idle());
    for (auto sql :
         array<ref<str>, 2> { "CREATE VIRTUAL TABLE search USING fts5(body,tokenize='trigram')"_str,
                              "INSERT INTO search VALUES('显存 C++ KV Cache')"_str }) {
        auto statement = first.prepare(sql).unwrap();
        EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Done);
    }
    EXPECT_TRUE(first.idle());
    EXPECT_TRUE(first.close().is_ok());
    EXPECT_FALSE(first.idle());
}

TEST(Sqlite, BindingTypesAndOwnedResults) {
    auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
    auto statement = db.prepare("SELECT ?1, ?2, ?3"_str).unwrap();
    EXPECT_EQ(statement.step().unwrap_err().kind(), sqlito::ErrorKind::Unbound);
    ASSERT_TRUE(statement.bind(usize(1), i64(42)).is_ok());
    ASSERT_TRUE(statement.bind_text(usize(2), "a'b\0c"_str).is_ok());
    ASSERT_TRUE(statement.bind_null(usize(3)).is_ok());
    EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*statement.integer(usize()).unwrap(), i64(42));
    auto text = statement.text(usize(1)).unwrap();
    EXPECT_TRUE(statement.integer(usize(2)).unwrap().is_none());
    EXPECT_EQ(statement.integer(usize(1)).unwrap_err().kind(), sqlito::ErrorKind::Type);
    EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Done);
    EXPECT_TRUE(statement.step().is_err());
    ASSERT_TRUE(statement.reset().is_ok());
    EXPECT_EQ(text->as_str(), "a'b\0c"_str);
    EXPECT_EQ(statement.step().unwrap_err().kind(), sqlito::ErrorKind::Unbound);
}
TEST(Sqlite, RealColumnTypeAndState) {
    auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
    auto statement = db.prepare("SELECT 1.25, NULL, 2, '3', x'04'"_str).unwrap();
    EXPECT_EQ(statement.real(usize()).unwrap_err().kind(), sqlito::ErrorKind::State);
    EXPECT_EQ(statement.column_type(usize()).unwrap_err().kind(), sqlito::ErrorKind::State);
    ASSERT_TRUE(statement.step().is_ok());
    EXPECT_EQ(statement.column_type(usize()).unwrap(), sqlito::ColumnType::Real);
    EXPECT_EQ(statement.column_type(usize(1)).unwrap(), sqlito::ColumnType::Null);
    EXPECT_EQ(statement.column_type(usize(2)).unwrap(), sqlito::ColumnType::Integer);
    EXPECT_EQ(statement.column_type(usize(3)).unwrap(), sqlito::ColumnType::Text);
    EXPECT_EQ(statement.column_type(usize(4)).unwrap(), sqlito::ColumnType::Blob);
    EXPECT_TRUE(statement.column_type(usize(5)).is_err());
    EXPECT_EQ(*statement.real(usize()).unwrap(), 1.25);
    EXPECT_TRUE(statement.real(usize(1)).unwrap().is_none());
    EXPECT_EQ(statement.real(usize(2)).unwrap_err().kind(), sqlito::ErrorKind::Type);
    EXPECT_EQ(statement.real(usize(3)).unwrap_err().kind(), sqlito::ErrorKind::Type);
    EXPECT_EQ(statement.real(usize(4)).unwrap_err().kind(), sqlito::ErrorKind::Type);
    EXPECT_TRUE(statement.real(usize(5)).is_err());
    EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(statement.real(usize()).unwrap_err().kind(), sqlito::ErrorKind::State);
}
TEST(Sqlite, StatementKeepsConnectionAliveAndCloseReportsBusy) {
    auto statement = [] {
        auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
        auto statement = db.prepare("SELECT 7"_str).unwrap();
        EXPECT_TRUE(db.close().is_err());
        return statement;
    }();
    EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*statement.integer(usize()).unwrap(), i64(7));
}
TEST(Sqlite, RejectsInvalidStateAndMultipleStatements) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    EXPECT_TRUE(db.prepare("SELECT 1; SELECT 2"_str).is_err());
    EXPECT_TRUE(db.prepare("SELECT 1\0;SELECT 2"_str).is_err());
    EXPECT_TRUE(db.prepare(""_str).is_err());
    {
        auto statement = db.prepare("SELECT ?"_str).unwrap();
        EXPECT_TRUE(statement.bind(usize(), i64()).is_err());
        EXPECT_TRUE(statement.integer(usize()).is_err());
        ASSERT_TRUE(statement.bind(usize(1), i64()).is_ok());
        ASSERT_TRUE(statement.step().is_ok());
        EXPECT_TRUE(statement.bind_null(usize(1)).is_err());
        EXPECT_TRUE(statement.integer(usize(1)).is_err());
    }
    EXPECT_TRUE(db.close().is_ok());
    EXPECT_TRUE(db.prepare("SELECT 1"_str).is_err());
}
TEST(Sqlite, WritesAndReportsConstraintFailure) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    auto create =
        db.prepare("CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT)"_str).unwrap();
    EXPECT_EQ(create.step().unwrap(), sqlito::Step::Done);
    auto insert = db.prepare("INSERT INTO notes VALUES (?1, ?2)"_str).unwrap();
    ASSERT_TRUE(insert.bind(usize(1), i64(1)).is_ok());
    ASSERT_TRUE(insert.bind_text(usize(2), "quoted ' title"_str).is_ok());
    EXPECT_EQ(insert.step().unwrap(), sqlito::Step::Done);
    ASSERT_TRUE(insert.reset().is_ok());
    ASSERT_TRUE(insert.bind(usize(1), i64(1)).is_ok());
    ASSERT_TRUE(insert.bind_null(usize(2)).is_ok());
    auto failed = insert.step();
    ASSERT_TRUE(failed.is_err());
    EXPECT_EQ(failed.unwrap_err().kind(), sqlito::ErrorKind::Database);
    EXPECT_TRUE(insert.step().is_err());
    EXPECT_TRUE(insert.reset().is_err());
    ASSERT_TRUE(insert.bind(usize(1), i64(2)).is_ok());
    ASSERT_TRUE(insert.bind_text(usize(2), ""_str).is_ok());
    EXPECT_EQ(insert.step().unwrap(), sqlito::Step::Done);
    auto query = db.prepare("SELECT title FROM notes WHERE id=1"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    auto title = query.text(usize()).unwrap();
    EXPECT_EQ(title->as_str(), "quoted ' title"_str);
}

TEST(Sqlite, EngineAndFts5AreAvailable) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
#if LITO_FEAT_VENDORED
    auto version = db.prepare("SELECT sqlite_version()"_str).unwrap();
    ASSERT_EQ(version.step().unwrap(), sqlito::Step::Row);
    auto value = version.text(usize()).unwrap();
    EXPECT_EQ(value->as_str(), "3.53.4"_str);
#endif
    auto create = db.prepare("CREATE VIRTUAL TABLE search USING fts5(body)"_str).unwrap();
    EXPECT_EQ(create.step().unwrap(), sqlito::Step::Done);
    auto insert = db.prepare("INSERT INTO search(body) VALUES (?)"_str).unwrap();
    ASSERT_TRUE(insert.bind_text(usize(1), "notebook rendering"_str).is_ok());
    EXPECT_EQ(insert.step().unwrap(), sqlito::Step::Done);
    auto query = db.prepare("SELECT count(*) FROM search WHERE search MATCH ?"_str).unwrap();
    ASSERT_TRUE(query.bind_text(usize(1), "rendering"_str).is_ok());
    EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*query.integer(usize()).unwrap(), i64(1));
}

TEST(Sqlite, LastChangesTracksConnectionRatherThanStatements) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY);"
                                 "INSERT INTO items VALUES(1),(2),(3);"_str)
                    .is_ok());
    EXPECT_EQ(db.last_changes().unwrap(), u64(3));
    auto select = db.prepare("SELECT id FROM items WHERE id=1"_str).unwrap();
    ASSERT_EQ(select.step().unwrap(), sqlito::Step::Row);
    ASSERT_EQ(select.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(3));
    ASSERT_TRUE(db.execute_batch("CREATE TABLE other(id INTEGER)"_str).is_ok());
    EXPECT_EQ(db.last_changes().unwrap(), u64(3));
    auto remove = db.prepare("DELETE FROM items WHERE id <= ?"_str).unwrap();
    ASSERT_TRUE(remove.bind(usize(1), i64(2)).is_ok());
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Done);
    auto count = db.last_changes().unwrap();
    EXPECT_EQ(count, u64(2));
    ASSERT_TRUE(remove.reset().is_ok());
    EXPECT_EQ(db.last_changes().unwrap(), u64(2));
    auto insert = db.prepare("INSERT INTO other VALUES(4)"_str).unwrap();
    ASSERT_EQ(insert.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(1));
    EXPECT_EQ(count, u64(2));
    ASSERT_TRUE(remove.bind(usize(1), i64(0)).is_ok());
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(0));
}

TEST(Sqlite, LastChangesExcludesTriggerAndForeignKeyChanges) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("PRAGMA foreign_keys=ON;"
                                 "CREATE TABLE parent(id INTEGER PRIMARY KEY);"
                                 "CREATE TABLE child(id INTEGER PRIMARY KEY, parent INTEGER "
                                 "REFERENCES parent(id) ON DELETE CASCADE);"
                                 "CREATE TABLE audit(id INTEGER);"
                                 "CREATE TRIGGER removed AFTER DELETE ON parent BEGIN "
                                 "INSERT INTO audit VALUES(old.id); END;"
                                 "INSERT INTO parent VALUES(1),(2);"
                                 "INSERT INTO child VALUES(10,1),(11,1),(12,2);"_str)
                    .is_ok());
    auto remove = db.prepare("DELETE FROM parent WHERE id=1"_str).unwrap();
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(1));
    auto check = db.prepare("SELECT (SELECT count(*) FROM child), "
                            "(SELECT count(*) FROM audit)"_str)
                     .unwrap();
    ASSERT_EQ(check.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*check.integer(usize()).unwrap(), i64(1));
    EXPECT_EQ(*check.integer(usize(1)).unwrap(), i64(1));
}

TEST(Sqlite, LastChangesReturningRequiresFinalStep) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY);"
                                 "CREATE TABLE other(id INTEGER);"
                                 "INSERT INTO items VALUES(1),(2);"_str)
                    .is_ok());
    auto remove = db.prepare("DELETE FROM items RETURNING id"_str).unwrap();
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(db.last_changes().unwrap(), u64(2));
    auto other = db.prepare("INSERT INTO other VALUES(1)"_str).unwrap();
    ASSERT_EQ(other.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(1));
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Row);
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(2));
}

TEST(Sqlite, LastChangesAfterFailureIsNotTheFailedStatementResult) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY);"
                                 "INSERT INTO items VALUES(1),(2);"_str)
                    .is_ok());
    auto failure = db.prepare("INSERT INTO items VALUES(1)"_str).unwrap();
    ASSERT_TRUE(failure.step().is_err());
    // SQLite's connection counter is not a success result for the failed statement.
    auto after_error = db.last_changes().unwrap();
    ASSERT_TRUE(failure.reset().is_err());
    EXPECT_EQ(db.last_changes().unwrap(), after_error);
    auto missing = db.prepare("DELETE FROM items WHERE id=999"_str).unwrap();
    ASSERT_EQ(missing.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(0));
}

TEST(Sqlite, LastChangesWithCountChangesPragma) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE items(id INTEGER);"
                                 "INSERT INTO items VALUES(1),(2),(3);"
                                 "PRAGMA count_changes=ON;"_str)
                    .is_ok());
    auto remove = db.prepare("DELETE FROM items WHERE id<3"_str).unwrap();
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Row);
    ASSERT_EQ(remove.step().unwrap(), sqlito::Step::Done);
    EXPECT_EQ(db.last_changes().unwrap(), u64(2));
}

TEST(Sqlite, BindsRealValues) {
    auto db   = sqlito::Connection::open(":memory:"_str).unwrap();
    auto stmt = db.prepare("SELECT ?1, typeof(?1), ?2"_str).unwrap();
    ASSERT_TRUE(stmt.bind(usize(1), 0.5).is_ok());
    ASSERT_TRUE(stmt.bind(usize(2), -1.25).is_ok());
    ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(stmt.real(usize()).unwrap().unwrap(), 0.5);
    EXPECT_TRUE(stmt.text(usize(1)).unwrap().unwrap().as_str() == "real"_str);
    EXPECT_EQ(stmt.real(usize(2)).unwrap().unwrap(), -1.25);
}

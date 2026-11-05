#include <rstd/test/gtest.hpp>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;

TEST(Returning, InsertUpdateAndNoMatch) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(
        db.execute_batch(
              "CREATE TABLE state(namespace TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL, data TEXT NOT NULL)"_str)
            .is_ok());
    {
        auto insert =
            db.prepare(
                  "INSERT INTO state VALUES(?1,1,?2) ON CONFLICT(namespace) DO NOTHING RETURNING revision,data"_str)
                .unwrap();
        EXPECT_EQ(insert.integer(usize()).unwrap_err().kind, sqlito::ErrorKind::State);
        ASSERT_TRUE(insert.bind_text(usize(1), "plugin"_str).is_ok());
        EXPECT_EQ(insert.step().unwrap_err().kind, sqlito::ErrorKind::Unbound);
        ASSERT_TRUE(insert.bind_text(usize(2), "中文\0opaque"_str).is_ok());
        ASSERT_EQ(insert.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(insert.integer(usize()).unwrap().unwrap(), i64(1));
        EXPECT_EQ(insert.integer(usize(1)).unwrap_err().kind, sqlito::ErrorKind::Type);
        auto stored = insert.text(usize(1)).unwrap().unwrap();
        EXPECT_EQ(stored.as_str(), "中文\0opaque"_str);
        EXPECT_FALSE(db.idle());
        ASSERT_EQ(insert.step().unwrap(), sqlito::Step::Done);
        EXPECT_EQ(insert.text(usize(1)).unwrap_err().kind, sqlito::ErrorKind::State);
        EXPECT_EQ(stored.as_str(), "中文\0opaque"_str);
        ASSERT_TRUE(insert.reset().is_ok());
        ASSERT_TRUE(insert.bind_text(usize(1), "plugin"_str).is_ok());
        ASSERT_TRUE(insert.bind_text(usize(2), "replacement"_str).is_ok());
        EXPECT_EQ(insert.step().unwrap(), sqlito::Step::Done);
        EXPECT_EQ(insert.integer(usize()).unwrap_err().kind, sqlito::ErrorKind::State);
    }
    EXPECT_TRUE(db.idle());
    {
        auto update =
            db.prepare(
                  "UPDATE state SET revision=revision+1,data=?1 WHERE namespace=?2 AND revision=?3 RETURNING revision,data"_str)
                .unwrap();
        ASSERT_TRUE(update.bind_text(usize(1), ""_str).is_ok());
        ASSERT_TRUE(update.bind_text(usize(2), "plugin"_str).is_ok());
        ASSERT_TRUE(update.bind(usize(3), i64(1)).is_ok());
        ASSERT_EQ(update.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(update.integer(usize()).unwrap().unwrap(), i64(2));
        EXPECT_EQ(update.text(usize(1)).unwrap().unwrap().as_str(), ""_str);
        ASSERT_EQ(update.step().unwrap(), sqlito::Step::Done);
        ASSERT_TRUE(update.reset().is_ok());
        ASSERT_TRUE(update.bind_text(usize(1), "stale"_str).is_ok());
        ASSERT_TRUE(update.bind_text(usize(2), "plugin"_str).is_ok());
        ASSERT_TRUE(update.bind(usize(3), i64(1)).is_ok());
        EXPECT_EQ(update.step().unwrap(), sqlito::Step::Done);
    }
    EXPECT_TRUE(db.idle());
    auto query =
        db.prepare("SELECT revision,data FROM state WHERE namespace='plugin'"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.integer(usize()).unwrap().unwrap(), i64(2));
    EXPECT_EQ(query.text(usize(1)).unwrap().unwrap().as_str(), ""_str);
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Done);
}

TEST(Returning, ErrorsNullAndInvalidUtf8) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(
        db.execute_batch(
              "CREATE TABLE state(id INTEGER PRIMARY KEY, value TEXT); CREATE TRIGGER reject BEFORE INSERT ON state WHEN NEW.value='reject' BEGIN SELECT RAISE(ABORT,'reject'); END;"_str)
            .is_ok());
    {
        auto stmt = db.prepare("INSERT INTO state VALUES(1,?1) RETURNING id,value"_str).unwrap();
        ASSERT_TRUE(stmt.bind_text(usize(1), "reject"_str).is_ok());
        EXPECT_EQ(stmt.step().unwrap_err().kind, sqlito::ErrorKind::Database);
        EXPECT_EQ(stmt.step().unwrap_err().kind, sqlito::ErrorKind::State);
        EXPECT_EQ(stmt.text(usize(1)).unwrap_err().kind, sqlito::ErrorKind::State);
    }
    {
        auto stmt = db.prepare("INSERT INTO state VALUES(1,NULL) RETURNING id,value"_str).unwrap();
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
        EXPECT_TRUE(stmt.text(usize(1)).unwrap().is_none());
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
    }
    {
        auto stmt =
            db.prepare("UPDATE state SET value=CAST(X'FF' AS TEXT) RETURNING value"_str).unwrap();
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(stmt.text(usize()).unwrap_err().kind, sqlito::ErrorKind::Type);
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
    }
    EXPECT_TRUE(db.idle());
}

TEST(Returning, TransactionCommitAndRollback) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE TABLE state(id INTEGER PRIMARY KEY,value TEXT)"_str).is_ok());
    {
        auto tx = db.begin().unwrap();
        {
            auto stmt = tx.prepare("INSERT INTO state VALUES(1,'kept') RETURNING id"_str).unwrap();
            ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
            EXPECT_EQ(stmt.integer(usize()).unwrap().unwrap(), i64(1));
            ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
        }
        ASSERT_TRUE(tx.commit().is_ok());
    }
    {
        auto tx = db.begin().unwrap();
        {
            auto stmt =
                tx.prepare("UPDATE state SET value='discarded' RETURNING value"_str).unwrap();
            ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
            ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
        }
        ASSERT_TRUE(tx.rollback().is_ok());
    }
    EXPECT_TRUE(db.idle());
    auto query = db.prepare("SELECT value FROM state WHERE id=1"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.text(usize()).unwrap().unwrap().as_str(), "kept"_str);
}

TEST(Returning, CancellationBetweenRowAndDoneDoesNotPromiseRollback) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE state(id INTEGER PRIMARY KEY)"_str).is_ok());
    auto cancel = sqlito::Cancellation::make();
    auto result = db.run(cancel, [&](auto& session) -> Result<sqlito::Step, sqlito::Error> {
        auto stmt = session.prepare("INSERT INTO state VALUES(1) RETURNING id"_str).unwrap();
        if (stmt.step().unwrap() != sqlito::Step::Row) return Ok(sqlito::Step::Done);
        cancel.cancel();
        return stmt.step();
    });
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    EXPECT_TRUE(db.idle());
    auto query = db.prepare("SELECT count(*) FROM state"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.integer(usize()).unwrap().unwrap(), i64(1));
}

TEST(Returning, CancellationBetweenRowAndDoneRollsBackExplicitTransaction) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE state(id INTEGER PRIMARY KEY)"_str).is_ok());
    auto cancel = sqlito::Cancellation::make();
    auto result = db.run(cancel, [&](auto& session) -> Result<sqlito::Step, sqlito::Error> {
        auto tx   = session.begin().unwrap();
        auto stmt = tx.prepare("INSERT INTO state VALUES(1) RETURNING id"_str).unwrap();
        if (stmt.step().unwrap() != sqlito::Step::Row) return Ok(sqlito::Step::Done);
        cancel.cancel();
        return stmt.step();
    });
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    EXPECT_TRUE(db.idle());
    auto query = db.prepare("SELECT count(*) FROM state"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.integer(usize()).unwrap().unwrap(), i64());
}

TEST(Returning, DeferredConstraintFailsCommitAfterRowsAndRollsBack) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    ASSERT_TRUE(
        db.execute_batch(
              "PRAGMA foreign_keys=ON; CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"_str)
            .is_ok());
    {
        auto tx = db.begin().unwrap();
        {
            auto statement = tx.prepare("INSERT INTO child VALUES(42) RETURNING id"_str).unwrap();
            ASSERT_EQ(statement.step().unwrap(), sqlito::Step::Row);
            ASSERT_EQ(statement.integer(usize()).unwrap().unwrap(), i64(42));
            ASSERT_EQ(statement.step().unwrap(), sqlito::Step::Done);
        }
        auto committed = tx.commit();
        ASSERT_TRUE(committed.is_err());
        EXPECT_EQ(committed.unwrap_err().kind, sqlito::ErrorKind::Database);
        ASSERT_TRUE(tx.rollback().is_ok());
    }
    EXPECT_TRUE(db.idle());
    auto query = db.prepare("SELECT COUNT(*) FROM child"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.integer(usize()).unwrap().unwrap(), i64());
}

TEST(Returning, BusyAtCompletionMustNotReportSuccess) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = rstd::path::PathBuf::from(directory.path());
    path.push(rstd::path::PathBuf::from("busy.sqlite"_str).as_path());
    auto name   = path.as_path().to_str().unwrap();
    auto reader = sqlito::Connection::open(name).unwrap();
    auto writer = sqlito::Connection::open(name).unwrap();
    ASSERT_TRUE(
        reader
            .execute_batch(
                "PRAGMA journal_mode=DELETE; CREATE TABLE state(id INTEGER PRIMARY KEY)"_str)
            .is_ok());
    {
        auto held = reader.prepare("SELECT COUNT(*) FROM state"_str).unwrap();
        ASSERT_EQ(held.step().unwrap(), sqlito::Step::Row);
        auto stmt = writer.prepare("INSERT INTO state VALUES(1) RETURNING id"_str).unwrap();
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(stmt.integer(usize()).unwrap().unwrap(), i64(1));
        auto done = stmt.step();
        ASSERT_TRUE(done.is_err());
        EXPECT_EQ(done.unwrap_err().kind, sqlito::ErrorKind::Database);
    }
    EXPECT_TRUE(reader.idle());
    EXPECT_TRUE(writer.idle());
    auto query = writer.prepare("SELECT count(*) FROM state"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.integer(usize()).unwrap().unwrap(), i64());
}

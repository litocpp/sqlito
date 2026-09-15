#include <rstd/test/gtest.hpp>
import sqlito;
using namespace rstd::prelude;
using namespace rstd::literals;

TEST(Cancellation, PreCancelledSkipsCallback) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto cancel = sqlito::Cancellation::make();
    cancel.cancel();
    bool called = false;
    auto result = db.run(cancel, [&](auto&) -> Result<empty, sqlito::Error> {
        called = true;
        return Ok(empty {});
    });
    EXPECT_FALSE(called);
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
}
TEST(Cancellation, RunningQueryStopsAndConnectionCanBeReused) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto cancel = sqlito::Cancellation::make();
    auto result = db.run(cancel, [&](auto& session) -> Result<empty, sqlito::Error> {
        auto query =
            session
                .prepare(
                    "WITH RECURSIVE numbers(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM numbers WHERE x<1000000000) SELECT sum(x) FROM numbers"_str)
                .unwrap();
        auto thread = rstd::thread::spawn([cancel = cancel.clone()] {
                          rstd::thread::sleep(rstd::time::Duration::from_millis(u64(30)));
                          cancel.cancel();
                      }).unwrap();
        auto step   = query.step();
        (void)rstd::move(thread).join().unwrap();
        if (step.is_err()) return Err(rstd::move(step).unwrap_err());
        return Ok(empty {});
    });
    ASSERT_TRUE(result.is_err());
    auto failure = result.unwrap_err();
    EXPECT_EQ(failure.kind, sqlito::ErrorKind::Cancelled);
    EXPECT_EQ(failure.code, 9);
    auto query = db.prepare("SELECT 1"_str).unwrap();
    EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
}
TEST(Cancellation, CancelledCommitStillRollsBack) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    {
        auto create = db.prepare("CREATE TABLE notes(id INTEGER)"_str).unwrap();
        ASSERT_TRUE(create.step().is_ok());
    }
    auto cancel = sqlito::Cancellation::make();
    auto result = db.run(cancel, [&](auto& session) -> Result<empty, sqlito::Error> {
        auto tx     = session.begin().unwrap();
        auto insert = tx.prepare("INSERT INTO notes VALUES (1)"_str).unwrap();
        EXPECT_TRUE(insert.step().is_ok());
        cancel.cancel();
        return tx.commit();
    });
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    auto tx    = db.begin().unwrap();
    auto count = tx.prepare("SELECT count(*) FROM notes"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
    ASSERT_TRUE(tx.rollback().is_ok());
}
TEST(Cancellation, ScopeRejectsNestedRunAndClose) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto cancel = sqlito::Cancellation::make();
    auto result = db.run(cancel, [&](auto& session) -> Result<empty, sqlito::Error> {
        EXPECT_TRUE(session.close().is_err());
        auto nested = session.run(cancel, [](auto&) -> Result<empty, sqlito::Error> {
            return Ok(empty {});
        });
        EXPECT_EQ(nested.unwrap_err().kind, sqlito::ErrorKind::State);
        return Ok(empty {});
    });
    ASSERT_TRUE(result.is_ok());
    ASSERT_TRUE(db.close().is_ok());
}

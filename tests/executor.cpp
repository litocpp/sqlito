#include <rstd/test/gtest.hpp>
import sqlito;
using namespace rstd::prelude;
using namespace rstd::literals;

struct Gate {
    struct Fields {
        bool started {}, released {};
    };
    rstd::sync::Mutex<Fields> fields { Fields {} };
    rstd::sync::Condvar       changed;
    void                      enter() {
        auto guard     = fields.lock().unwrap();
        guard->started = true;
        changed.notify_all();
        changed.wait_while(guard, [](const Fields& value) {
            return ! value.released;
        });
    }
    void wait_started() {
        auto guard = fields.lock().unwrap();
        changed.wait_while(guard, [](const Fields& value) {
            return ! value.started;
        });
    }
    void release() {
        {
            auto guard      = fields.lock().unwrap();
            guard->released = true;
        }
        changed.notify_all();
    }
};
auto value(sqlito::Connection& db) -> Result<i64, sqlito::Error> {
    auto prepared = db.prepare("SELECT 42"_str);
    if (prepared.is_err()) return Err(rstd::move(prepared).unwrap_err());
    auto query = rstd::move(prepared).unwrap();
    auto step  = query.step();
    if (step.is_err()) return Err(rstd::move(step).unwrap_err());
    return Ok(query.integer(usize()).unwrap().unwrap());
}
TEST(Executor, FtsStatementsDoNotPoisonWorker) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str).unwrap();
    auto handle   = executor.handle();
    for (auto sql : array<ref<str>, 3> { "CREATE VIRTUAL TABLE search USING fts5(body)"_str,
                                         "INSERT INTO search VALUES('rendering')"_str,
                                         "DELETE FROM search"_str }) {
        auto operation = handle
                             .submit([sql](auto& db) -> Result<empty, sqlito::Error> {
                                 auto statement = db.prepare(sql);
                                 if (statement.is_err())
                                     return Err(rstd::move(statement).unwrap_err());
                                 auto result = statement->step();
                                 if (result.is_err()) return Err(rstd::move(result).unwrap_err());
                                 return Ok(empty {});
                             })
                             .unwrap();
        ASSERT_TRUE(runtime.block_on(rstd::move(operation)).is_ok());
    }
    EXPECT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, AwaitResultAndExplicitShutdown) {
    auto runtime   = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor  = sqlito::Executor::open(":memory:"_str).unwrap();
    auto handle    = executor.handle();
    auto operation = handle.submit(value).unwrap();
    auto result    = runtime.block_on(rstd::move(operation));
    ASSERT_TRUE(result.is_ok());
    EXPECT_EQ(*result, i64(42));
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
    EXPECT_TRUE(handle.submit(value).is_err());
    EXPECT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, DroppedOperationRetainsCapacityUntilWorkerCleansUp) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str, usize(1)).unwrap();
    auto handle   = executor.handle();
    auto gate     = rstd::sync::Arc<Gate>::make();
    {
        auto operation = handle
                             .submit([gate = gate.clone()](auto& db) {
                                 gate->enter();
                                 return value(db);
                             })
                             .unwrap();
        gate->wait_started();
    }
    auto full = handle.submit(value);
    EXPECT_EQ(full.unwrap_err().kind, sqlito::ErrorKind::Busy);
    gate->release();
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, CancelledQueueEntryDoesNotRunAndStopRejectsAdmission) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str, usize(2)).unwrap();
    auto handle   = executor.handle();
    auto gate     = rstd::sync::Arc<Gate>::make();
    auto first    = handle
                        .submit([gate = gate.clone()](auto& db) {
                         gate->enter();
                         return value(db);
                        })
                        .unwrap();
    gate->wait_started();
    auto called = rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>>::make(false);
    auto second = handle
                      .submit([called = called.clone()](auto& db) {
                          called->store(true, rstd::sync::atomic::Ordering::Release);
                          return value(db);
                      })
                      .unwrap();
    second.cancel();
    EXPECT_EQ(handle.submit(value).unwrap_err().kind, sqlito::ErrorKind::Busy);
    gate->release();
    ASSERT_TRUE(runtime.block_on(rstd::move(first)).is_ok());
    auto cancelled = runtime.block_on(rstd::move(second));
    EXPECT_EQ(cancelled.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    EXPECT_FALSE(called->load(rstd::sync::atomic::Ordering::Acquire));
    auto next = handle.submit(value).unwrap();
    ASSERT_TRUE(runtime.block_on(rstd::move(next)).is_ok());
    executor.request_stop();
    EXPECT_TRUE(handle.submit(value).is_err());
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, TransactionIsCleanBeforeCompletion) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str, usize(1)).unwrap();
    auto handle   = executor.handle();
    auto write    = handle
                        .submit([](auto& db) -> Result<empty, sqlito::Error> {
                         auto tx     = db.begin().unwrap();
                         auto create = tx.prepare("CREATE TABLE notes(id INTEGER)"_str).unwrap();
                         create.step().unwrap();
                         auto insert = tx.prepare("INSERT INTO notes VALUES(7)"_str).unwrap();
                         insert.step().unwrap();
                         return tx.commit();
                        })
                        .unwrap();
    ASSERT_TRUE(runtime.block_on(rstd::move(write)).is_ok());
    auto read = handle
                    .submit([](auto& db) -> Result<i64, sqlito::Error> {
                        auto query = db.prepare("SELECT id FROM notes"_str).unwrap();
                        query.step().unwrap();
                        return Ok(query.integer(usize()).unwrap().unwrap());
                    })
                    .unwrap();
    EXPECT_EQ(runtime.block_on(rstd::move(read)).unwrap(), i64(7));
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
struct Cleanup {
    rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>> done;
    explicit Cleanup(rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>> value)
        : done(rstd::move(value)) {}
    ~Cleanup() { done->store(true, rstd::sync::atomic::Ordering::Release); }
};
TEST(Executor, ConcurrentSubmittersShareCapacityAndStopCancelsQueuedWork) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str, usize(3)).unwrap();
    auto handle   = executor.handle();
    auto gate     = rstd::sync::Arc<Gate>::make();
    auto first    = handle
                        .submit([gate = gate.clone()](auto& db) {
                         gate->enter();
                         return value(db);
                        })
                        .unwrap();
    gate->wait_started();
    auto left   = rstd::thread::spawn([handle = handle.clone()]() mutable {
                    return handle.submit(value);
                  }).unwrap();
    auto right  = rstd::thread::spawn([handle = handle.clone()]() mutable {
                     return handle.submit(value);
                  }).unwrap();
    auto second = rstd::move(left).join().unwrap().unwrap();
    auto third  = rstd::move(right).join().unwrap().unwrap();
    EXPECT_EQ(handle.submit(value).unwrap_err().kind, sqlito::ErrorKind::Busy);
    executor.request_stop();
    EXPECT_EQ(handle.submit(value).unwrap_err().kind, sqlito::ErrorKind::State);
    gate->release();
    EXPECT_EQ(runtime.block_on(rstd::move(first)).unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    EXPECT_EQ(runtime.block_on(rstd::move(second)).unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    EXPECT_EQ(runtime.block_on(rstd::move(third)).unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, CallbackDestroyedBeforeResultAndCapacityReuse) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str, usize(1)).unwrap();
    auto handle   = executor.handle();
    for (int i = 0; i < 12; ++i) {
        auto done      = rstd::sync::Arc<rstd::sync::atomic::Atomic<bool>>::make(false);
        auto cleanup   = alloc::boxed::Box<Cleanup>::make(done.clone());
        auto operation = handle
                             .submit([cleanup = rstd::move(cleanup)](auto& db) {
                                 return value(db);
                             })
                             .unwrap();
        ASSERT_TRUE(runtime.block_on(rstd::move(operation)).is_ok());
        EXPECT_TRUE(done->load(rstd::sync::atomic::Ordering::Acquire));
    }
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, ShutdownInterruptsRunningQueryAndCompletesAfterCleanup) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str).unwrap();
    auto gate     = rstd::sync::Arc<Gate>::make();
    auto operation =
        executor.handle()
            .submit([gate = gate.clone()](auto& db) -> Result<empty, sqlito::Error> {
                auto query =
                    db.prepare(
                          "WITH RECURSIVE numbers(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM numbers WHERE x<1000000000) SELECT sum(x) FROM numbers"_str)
                        .unwrap();
                gate->enter();
                auto result = query.step();
                if (result.is_err()) return Err(rstd::move(result).unwrap_err());
                return Ok(empty {});
            })
            .unwrap();
    gate->wait_started();
    gate->release();
    (void)runtime.block_on(rstd::async::sleep(rstd::time::Duration::from_millis(u64(30))));
    executor.request_stop();
    auto result = runtime.block_on(rstd::move(operation));
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}
TEST(Executor, RejectsEscapedStatementAndRecoversAfterItsDestruction) {
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor = sqlito::Executor::open(":memory:"_str).unwrap();
    auto handle   = executor.handle();
    auto bad      = handle
                        .submit([](auto& db) {
                       return db.prepare("SELECT 1"_str);
                        })
                        .unwrap();
    auto result   = runtime.block_on(rstd::move(bad));
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::State);
    auto next = handle.submit(value).unwrap();
    EXPECT_EQ(runtime.block_on(rstd::move(next)).unwrap(), i64(42));
    ASSERT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
}

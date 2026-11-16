#include <sqlite3.h>
#include <climits>
#include <rstd/test/gtest.hpp>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::sync::Arc;
using rstd::sync::atomic::Atomic;
using rstd::sync::atomic::Ordering;

TEST(SqliteMemory, ShutdownRetainsStatisticsAndTrimReleasesMappings) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    {
        auto db = sqlito::Connection::open(":memory:"_str).unwrap();
        db.execute_batch("CREATE TABLE notes(value INTEGER); INSERT INTO notes VALUES(42)"_str)
            .unwrap();
        auto query = db.prepare("SELECT value FROM notes"_str).unwrap();
        EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(*query.integer(usize()).unwrap(), i64(42));
        EXPECT_EQ(owner.shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
        auto before = owner.snapshot();
        EXPECT_GT(before.pool.live_bytes, usize());
        EXPECT_GT(before.upstream.mapped_bytes, usize());
        (void)owner.trim();
        EXPECT_EQ(*query.integer(usize()).unwrap(), i64(42));
    }
    ASSERT_TRUE(owner.shutdown().is_ok());
    EXPECT_EQ(owner.snapshot().pool.live_bytes, usize());
    EXPECT_EQ(owner.snapshot().pool.live_block_bytes, usize());
    (void)owner.trim();
    EXPECT_EQ(owner.snapshot().pool.reserved_bytes, usize());
    EXPECT_EQ(owner.snapshot().upstream.mapped_bytes, usize());
    EXPECT_TRUE(owner.shutdown().is_ok());
    auto next = sqlito::MemoryRuntime::create().unwrap();
    EXPECT_TRUE(next.shutdown().is_ok());
    EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(SqliteMemory, StatementAndBlobKeepThePoolAfterOwnerAndConnectionDrop) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    {
        auto query = [] {
            auto owner = sqlito::MemoryRuntime::create().unwrap();
            auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
            return db.prepare("SELECT 73"_str).unwrap();
        }();
        EXPECT_EQ(sqlito::MemoryRuntime::create().unwrap_err().kind(), sqlito::ErrorKind::Busy);
        EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(*query.integer(usize()).unwrap(), i64(73));
    }
    {
        auto blob = [] {
            auto owner = sqlito::MemoryRuntime::create().unwrap();
            auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
            db.execute_batch("CREATE TABLE data(payload BLOB);"
                             "INSERT INTO data VALUES(zeroblob(32))"_str)
                .unwrap();
            return db.open_blob("main"_str, "data"_str, "payload"_str, i64(1)).unwrap();
        }();
        EXPECT_EQ(blob.len().unwrap(), usize(32));
        EXPECT_EQ(sqlito::MemoryRuntime::create().unwrap_err().kind(), sqlito::ErrorKind::Busy);
    }
    auto next = sqlito::MemoryRuntime::create().unwrap();
    EXPECT_TRUE(next.shutdown().is_ok());
}

TEST(SqliteMemory, ClosedConnectionReleasesLeaseBeforeItsWrapperIsDestroyed) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
    db.close().unwrap();
    EXPECT_TRUE(owner.shutdown().is_ok());
    auto next = sqlito::MemoryRuntime::create().unwrap();
    EXPECT_TRUE(db.close().is_ok());
    EXPECT_TRUE(next.shutdown().is_ok());
}

TEST(SqliteMemory, RejectsDuplicateLateAndInvalidConfiguration) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    {
        auto owner                  = sqlito::MemoryRuntime::create().unwrap();
        auto options                = sqlito::MemoryOptions {};
        options.pool.retained_bytes = usize();
        EXPECT_EQ(sqlito::MemoryRuntime::create(options).unwrap_err().kind(),
                  sqlito::ErrorKind::Busy);
        owner.shutdown().unwrap();
    }
    {
        auto db = sqlito::Connection::open(":memory:"_str).unwrap();
        EXPECT_EQ(sqlito::MemoryRuntime::create().unwrap_err().kind(), sqlito::ErrorKind::State);
        EXPECT_TRUE(db.execute_batch("CREATE TABLE notes(value INTEGER)"_str).is_ok());
    }
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto invalid                  = sqlito::MemoryOptions {};
    invalid.pool.slab_granularity = usize(3);
    EXPECT_EQ(sqlito::MemoryRuntime::create(invalid).unwrap_err().kind(), sqlito::ErrorKind::Input);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(SqliteMemory, InitializationFailureRestoresPreviousAllocator) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto options                        = sqlito::MemoryOptions {};
    options.pool.slab_bytes             = usize::MAX;
    options.pool.target_blocks_per_slab = usize();
    auto failed                         = sqlito::MemoryRuntime::create(options);
    ASSERT_TRUE(failed.is_err());
    EXPECT_EQ(failed.unwrap_err().code(), SQLITE_NOMEM);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
    EXPECT_TRUE(db.execute_batch("CREATE TABLE data(value INTEGER)"_str).is_ok());
    db.close().unwrap();
    EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(SqliteMemory, ReallocOomPreservesDataAndFailedOpenReleasesLease) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto  owner = sqlito::MemoryRuntime::create().unwrap();
    auto* bytes = static_cast<unsigned char*>(sqlite3_malloc(87368));
    ASSERT_NE(bytes, nullptr);
    EXPECT_EQ(reinterpret_cast<rstd::uintptr_t>(bytes) % 16, rstd::uintptr_t());
    EXPECT_GE(sqlite3_msize(bytes), sqlite3_uint64(87368));
    bytes[0]     = 79;
    auto  limit  = sqlite3_hard_heap_limit64(sqlite3_memory_used());
    auto  before = owner.snapshot();
    auto* grown  = sqlite3_realloc(bytes, 262144);
    EXPECT_EQ(grown, nullptr);
    EXPECT_EQ(bytes[0], 79);
    EXPECT_EQ(owner.snapshot().adapter.moved_reallocations, before.adapter.moved_reallocations);
    EXPECT_EQ(owner.snapshot().adapter.copied_bytes, before.adapter.copied_bytes);
    auto failed = sqlito::Connection::open(":memory:"_str);
    EXPECT_TRUE(failed.is_err());
    (void)sqlite3_hard_heap_limit64(limit);
    sqlite3_free(bytes);
    EXPECT_EQ(sqlite3_malloc64(sqlite3_uint64(INT_MAX) + 1), nullptr);
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    db.close().unwrap();
    EXPECT_TRUE(owner.shutdown().is_ok());
    EXPECT_EQ(owner.snapshot().pool.live_bytes, usize());
}

TEST(SqliteMemory, MovingReallocPreservesUsableCapacityAfterInPlaceShrink) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto  owner    = sqlito::MemoryRuntime::create().unwrap();
    auto* original = static_cast<unsigned char*>(sqlite3_malloc(87368));
    ASSERT_NE(original, nullptr);
    auto  capacity = sqlite3_msize(original);
    auto* shrunk   = static_cast<unsigned char*>(sqlite3_realloc(original, 1024));
    ASSERT_NE(shrunk, nullptr);
    EXPECT_EQ(shrunk, original);
    EXPECT_EQ(sqlite3_msize(shrunk), capacity);
    auto before = owner.snapshot();
    for (sqlite3_uint64 i = 0; i < capacity; ++i) shrunk[i] = static_cast<unsigned char>(i % 251);
    auto* grown = static_cast<unsigned char*>(sqlite3_realloc64(shrunk, capacity + 32768));
    ASSERT_NE(grown, nullptr);
    EXPECT_NE(grown, shrunk);
    bool preserved = true;
    for (sqlite3_uint64 i = 0; i < capacity; ++i)
        if (grown[i] != static_cast<unsigned char>(i % 251)) preserved = false;
    EXPECT_TRUE(preserved);
    auto after = owner.snapshot();
    EXPECT_EQ(after.adapter.moved_reallocations, before.adapter.moved_reallocations + usize(1));
    EXPECT_EQ(after.adapter.copied_bytes, before.adapter.copied_bytes + usize(capacity));
    EXPECT_EQ(after.pool.copied_bytes, before.pool.copied_bytes);
    EXPECT_EQ(after.pool.grows, before.pool.grows);
    sqlite3_free(grown);
    EXPECT_TRUE(owner.shutdown().is_ok());
    EXPECT_EQ(owner.snapshot().pool.live_bytes, usize());
}

TEST(SqliteMemory, ConcurrentConnectionsAndOwnerDropKeepWorkerLeases) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto                                ready   = Arc<Atomic<unsigned>>::make(0);
    auto                                release = Arc<Atomic<bool>>::make(false);
    Vec<rstd::thread::JoinHandle<bool>> threads;
    {
        auto owner = sqlito::MemoryRuntime::create().unwrap();
        for (unsigned i = 0; i < 4; ++i) {
            threads.push(rstd::thread::spawn([ready = ready.clone(), release = release.clone()] {
                             auto opened = sqlito::Connection::open(":memory:"_str);
                             ready->fetch_add(1, Ordering::Release);
                             while (! release->load(Ordering::Acquire)) rstd::thread::yield_now();
                             if (opened.is_err()) return false;
                             auto db = rstd::move(opened).unwrap();
                             return db
                                 .execute_batch("CREATE TABLE data(value INTEGER);"
                                                "INSERT INTO data VALUES(1)"_str)
                                 .is_ok();
                         }).unwrap());
        }
        while (ready->load(Ordering::Acquire) != 4) rstd::thread::yield_now();
        EXPECT_EQ(owner.shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
    }
    release->store(true, Ordering::Release);
    for (auto& thread : threads) EXPECT_TRUE(rstd::move(thread).join().unwrap());
    auto next = sqlito::MemoryRuntime::create().unwrap();
    EXPECT_TRUE(next.shutdown().is_ok());
}

TEST(SqliteMemory, ExecutorKeepsRuntimeAndClosesBeforeShutdown) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto owner     = sqlito::MemoryRuntime::create().unwrap();
    auto runtime   = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto executor  = sqlito::Executor::open(":memory:"_str).unwrap();
    auto operation = executor.handle()
                         .submit([](auto& db) {
                             return db.execute_batch("CREATE TABLE data(value INTEGER)"_str);
                         })
                         .unwrap();
    EXPECT_TRUE(runtime.block_on(rstd::move(operation)).is_ok());
    EXPECT_EQ(owner.shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
    EXPECT_TRUE(runtime.block_on(executor.shutdown()).is_ok());
    EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(SqliteMemory, StatementAndBlobPreventShutdownAfterConnectionDrop) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    {
        auto query = [] {
            auto db = sqlito::Connection::open(":memory:"_str).unwrap();
            return db.prepare("SELECT 5"_str).unwrap();
        }();
        EXPECT_EQ(owner.shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
        EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
    }
    {
        auto blob = [] {
            auto db = sqlito::Connection::open(":memory:"_str).unwrap();
            db.execute_batch("CREATE TABLE data(payload BLOB);"
                             "INSERT INTO data VALUES(zeroblob(32))"_str)
                .unwrap();
            return db.open_blob("main"_str, "data"_str, "payload"_str, i64(1)).unwrap();
        }();
        EXPECT_EQ(owner.shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
        EXPECT_EQ(blob.len().unwrap(), usize(32));
    }
    EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(SqliteMemory, ConfigurationRacesWithConnectionOpenWithoutReplacingLiveAllocator) {
    for (unsigned i = 0; i < 12; ++i) {
        ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
        auto start     = Arc<Atomic<bool>>::make(false);
        auto installer = rstd::thread::spawn([start = start.clone()] {
                             while (! start->load(Ordering::Acquire)) rstd::thread::yield_now();
                             return sqlito::MemoryRuntime::create();
                         }).unwrap();
        auto opener    = rstd::thread::spawn([start = start.clone()] {
                          while (! start->load(Ordering::Acquire)) rstd::thread::yield_now();
                          return sqlito::Connection::open(":memory:"_str);
                         }).unwrap();
        start->store(true, Ordering::Release);
        auto owner  = rstd::move(installer).join().unwrap();
        auto opened = rstd::move(opener).join().unwrap();
        ASSERT_TRUE(opened.is_ok());
        auto db = rstd::move(opened).unwrap();
        EXPECT_TRUE(db.execute_batch("CREATE TABLE data(value INTEGER)"_str).is_ok());
        if (owner.is_ok())
            EXPECT_EQ(owner->shutdown().unwrap_err().kind(), sqlito::ErrorKind::Busy);
        else
            EXPECT_EQ(owner.unwrap_err().kind(), sqlito::ErrorKind::State);
        db.close().unwrap();
        if (owner.is_ok()) EXPECT_TRUE(owner->shutdown().is_ok());
    }
}

TEST(SqliteMemory, PoolIsThreadSafeWithoutSqliteMemoryStatusSerialization) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    ASSERT_EQ(sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0), SQLITE_OK);
    {
        auto                                owner  = sqlito::MemoryRuntime::create().unwrap();
        auto                                before = owner.snapshot();
        Vec<rstd::thread::JoinHandle<bool>> threads;
        for (unsigned i = 0; i < 4; ++i) {
            threads.push(rstd::thread::spawn([] {
                             for (unsigned j = 0; j < 128; ++j) {
                                 auto* bytes = static_cast<unsigned char*>(sqlite3_malloc(65552));
                                 if (! bytes) return false;
                                 bytes[0] = 81;
                                 auto* grown =
                                     static_cast<unsigned char*>(sqlite3_realloc(bytes, 87368));
                                 if (! grown) {
                                     sqlite3_free(bytes);
                                     return false;
                                 }
                                 bool valid = grown[0] == 81;
                                 sqlite3_free(grown);
                                 if (! valid) return false;
                             }
                             return true;
                         }).unwrap());
        }
        for (auto& thread : threads) EXPECT_TRUE(rstd::move(thread).join().unwrap());
        EXPECT_GT(owner.snapshot().pool.reuses, usize());
        EXPECT_EQ(owner.snapshot().adapter.moved_reallocations,
                  before.adapter.moved_reallocations + usize(512));
        EXPECT_GT(owner.snapshot().adapter.copied_bytes, before.adapter.copied_bytes);
        EXPECT_TRUE(owner.shutdown().is_ok());
        EXPECT_EQ(owner.snapshot().pool.live_bytes, usize());
    }
    EXPECT_EQ(sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 1), SQLITE_OK);
}

TEST(SqliteMemory, ControlStorageAndSlabMappingSizesAreAccountedSeparately) {
    auto virtual_memory = rstd::alloc::VirtualMemoryAllocator {};
    auto page_size      = virtual_memory.page_size();
    for (auto granularity : array<usize, 2> { usize(1), page_size * usize(4) }) {
        ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
        auto options                  = sqlito::MemoryOptions {};
        options.pool.slab_granularity = granularity;
        options.pool.retained_bytes   = page_size * usize(9) + usize(1);
        auto owner                    = sqlito::MemoryRuntime::create(options).unwrap();
        auto initial                  = owner.snapshot();
        EXPECT_GT(initial.control_layout_bytes, usize());
        EXPECT_GT(initial.owner_bytes, usize());
        auto expected_control =
            rstd::alloc::Layout::from_size_align(initial.control_layout_bytes, page_size)
                .unwrap()
                .pad_to_align()
                .size();
        EXPECT_EQ(initial.control_bytes, expected_control);
        auto* allocation = sqlite3_malloc(87368);
        ASSERT_NE(allocation, nullptr);
        auto active = owner.snapshot();
        EXPECT_EQ(active.pool.reserved_bytes, active.upstream.mapped_bytes);
        EXPECT_EQ(active.pool.reserved_bytes % granularity.max(page_size), usize());
        sqlite3_free(allocation);
        owner.shutdown().unwrap();
        auto stopped = owner.snapshot();
        EXPECT_EQ(stopped.pool.reserved_bytes, stopped.upstream.mapped_bytes);
        EXPECT_EQ(stopped.pool.reserved_bytes, stopped.pool.retained_bytes);
        EXPECT_LE(stopped.pool.retained_bytes, options.pool.retained_bytes);
        EXPECT_EQ(stopped.pool.reserved_bytes % granularity.max(page_size), usize());
        (void)owner.trim();
        auto trimmed = owner.snapshot();
        EXPECT_EQ(trimmed.pool.reserved_bytes, usize());
        EXPECT_EQ(trimmed.upstream.mapped_bytes, usize());
        EXPECT_EQ(trimmed.control_layout_bytes, initial.control_layout_bytes);
        EXPECT_EQ(trimmed.control_bytes, initial.control_bytes);
        EXPECT_EQ(trimmed.owner_bytes, initial.owner_bytes);
    }
}

TEST(SqliteMemory, ReturningWritesReuseHotSlabsAfterColdSizeClasses) {
    ASSERT_EQ(sqlite3_shutdown(), SQLITE_OK);
    auto owner = sqlito::MemoryRuntime::create().unwrap();
    {
        auto db = sqlito::Connection::open(":memory:"_str).unwrap();
        db.execute_batch("CREATE TABLE records(id INTEGER PRIMARY KEY, title TEXT NOT NULL);"
                         "CREATE VIRTUAL TABLE records_fts USING fts5(title, content='records', "
                         "content_rowid='id');"
                         "CREATE TRIGGER records_insert AFTER INSERT ON records BEGIN "
                         "INSERT INTO records_fts(rowid,title) VALUES(new.id,new.title); END;"_str)
            .unwrap();
        auto report = [&](ref<str> phase, const sqlito::MemoryStatistics& before) {
            auto after = owner.snapshot();
            rstd::io::println(
                "[sqlite-memory] phase={} pool_allocations={} pool_reuses={} "
                "upstream_allocation_calls={} upstream_deallocation_calls={} retained_bytes={}",
                phase,
                after.pool.allocations - before.pool.allocations,
                after.pool.reuses - before.pool.reuses,
                after.upstream.allocation_calls - before.upstream.allocation_calls,
                after.upstream.deallocation_calls - before.upstream.deallocation_calls,
                after.pool.retained_bytes);
            return after.upstream.allocation_calls - before.upstream.allocation_calls;
        };
        auto write = [&] {
            auto statement =
                db.prepare(
                      "INSERT INTO records(title) VALUES('rendering allocation sample') RETURNING id,title"_str)
                    .unwrap();
            EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Row);
            EXPECT_TRUE(statement.integer(usize()).unwrap().is_some());
            EXPECT_EQ(statement.step().unwrap(), sqlito::Step::Done);
        };
        auto probe_hot = [&](ref<str> phase, int size) {
            auto before = owner.snapshot();
            for (unsigned i = 0; i < 32; ++i) {
                auto* block = sqlite3_malloc(size);
                ASSERT_NE(block, nullptr);
                sqlite3_free(block);
            }
            EXPECT_LT(report(phase, before), usize(32));
        };
        for (unsigned i = 0; i < 8; ++i) write();
        auto warm = owner.snapshot();
        for (unsigned i = 0; i < 32; ++i) write();
        EXPECT_LT(report("returning-warm"_str, warm), usize(32));
        probe_hot("hot-65552-warm"_str, 65552);
        probe_hot("hot-87368-warm"_str, 87368);
        (void)owner.trim();
        // Leave empty cold classes resident before the next returning statements.
        for (int size = 4096; size <= 65536; size += 1024) {
            auto* block = sqlite3_malloc(size);
            ASSERT_NE(block, nullptr);
            sqlite3_free(block);
        }
        auto cold = owner.snapshot();
        rstd::io::println("[sqlite-memory] phase=cold-ready retained_bytes={}",
                          cold.pool.retained_bytes);
        for (unsigned i = 0; i < 32; ++i) write();
        EXPECT_LT(report("returning-after-cold"_str, cold), usize(32));
        probe_hot("hot-65552-after-cold"_str, 65552);
        probe_hot("hot-87368-after-cold"_str, 87368);
        auto matches =
            db.prepare("SELECT count(*) FROM records_fts WHERE records_fts MATCH 'rendering'"_str)
                .unwrap();
        EXPECT_EQ(matches.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(*matches.integer(usize()).unwrap(), i64(72));
    }
    EXPECT_GT(owner.snapshot().pool.reuses, usize());
    owner.shutdown().unwrap();
    EXPECT_EQ(owner.snapshot().pool.live_bytes, usize());
    EXPECT_EQ(owner.snapshot().pool.live_block_bytes, usize());
    (void)owner.trim();
    EXPECT_EQ(owner.snapshot().upstream.mapped_bytes, usize());
}

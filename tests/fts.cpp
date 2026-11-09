#include <rstd/test/gtest.hpp>
#include <initializer_list>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;

auto simple_factory() -> sqlito::FtsFactory {
    return sqlito::FtsFactory::make([](slice<sqlito::FtsArgument> args) noexcept
                                        -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
        if (! args.is_empty())
            return Err(sqlito::FtsFailure {
                sqlito::ErrorKind::Input, 0, String::make("unexpected arguments"_str) });
        return Ok(sqlito::FtsTokenizer::make(
            [](const sqlito::FtsInput& input,
               sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                if (input.text().is_empty()) return Ok(empty {});
                return sink.emit(input.text(), usize(), input.text().len());
            }));
    });
}

TEST(Fts, RegistrationOwnerAndConnectionIsolation) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("whole"_str), simple_factory()).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='whole');"
                                 "INSERT INTO docs VALUES('hello');"_str)
                    .is_ok());
    auto query = db.prepare("SELECT count(*) FROM docs WHERE docs MATCH 'hello'"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*query.integer(usize()).unwrap(), i64(1));
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Done);

    auto other = sqlito::Connection::open(":memory:"_str).unwrap();
    EXPECT_TRUE(
        other.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='whole')"_str)
            .is_err());
}

TEST(Fts, DroppingOwnerMakesFurtherCallbacksFail) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    {
        auto registration =
            db.register_or_replace_fts5_tokenizer(String::make("whole"_str), simple_factory())
                .unwrap();
        ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='whole');"
                                     "INSERT INTO docs VALUES('hello');"_str)
                        .is_ok());
    }
    auto result = db.execute_batch("INSERT INTO docs VALUES('another')"_str);
    ASSERT_TRUE(result.is_err());
    auto failure = result.unwrap_err();
    ASSERT_TRUE(failure.callback().is_some());
    EXPECT_EQ(failure.callback()->kind, sqlito::ErrorKind::State);
}
TEST(Fts, ReplacementKeepsPriorLiveInstancesSeparate) {
    auto                            db = sqlito::Connection::open(":memory:"_str).unwrap();
    Option<sqlito::FtsRegistration> original =
        Some(db.register_or_replace_fts5_tokenizer(String::make("whole"_str), simple_factory())
                 .unwrap());
    ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE first USING fts5(text, tokenize='whole');"
                                 "INSERT INTO first VALUES('hello')"_str)
                    .is_ok());
    auto replacement =
        db.register_or_replace_fts5_tokenizer(String::make("whole"_str), simple_factory()).unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE second USING fts5(text, tokenize='whole');"
                                 "INSERT INTO second VALUES('other')"_str)
                    .is_ok());
    original = None();
    EXPECT_TRUE(db.execute_batch("INSERT INTO first VALUES('expired')"_str).is_err());
    EXPECT_TRUE(db.execute_batch("INSERT INTO second VALUES('new')"_str).is_ok());
}

struct DropResources {
    rstd::sync::Mutex<Option<sqlito::Statement>>   statement { None() };
    rstd::sync::Mutex<Option<sqlito::Transaction>> transaction { None() };
};
auto dropping_factory(rstd::sync::Arc<DropResources> resources) -> sqlito::FtsFactory {
    return sqlito::FtsFactory::make(
        [resources = rstd::move(resources)](slice<sqlito::FtsArgument>) noexcept
            -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [resources = resources.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    {
                        auto value = resources->statement.lock().unwrap();
                        *value     = None();
                    }
                    {
                        auto value = resources->transaction.lock().unwrap();
                        *value     = None();
                    }
                    if (input.text().is_empty()) return Ok(empty {});
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
}

TEST(Fts, CallbackReleasesStatementAndTransactionWithoutReentry) {
    auto db           = sqlito::Connection::open(":memory:"_str).unwrap();
    auto resources    = rstd::sync::Arc<DropResources>::make();
    auto registration = db.register_or_replace_fts5_tokenizer(String::make("dropper"_str),
                                                              dropping_factory(resources.clone()))
                            .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='dropper')"_str)
            .is_ok());
    auto tx     = db.begin().unwrap();
    auto extra  = tx.prepare("SELECT 1"_str).unwrap();
    auto insert = tx.prepare("INSERT INTO docs VALUES('hello')"_str).unwrap();
    {
        auto value = resources->statement.lock().unwrap();
        *value     = Some(rstd::move(extra));
    }
    {
        auto value = resources->transaction.lock().unwrap();
        *value     = Some(rstd::move(tx));
    }
    auto result = insert.step();
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind(), sqlito::ErrorKind::State);
    auto count = db.prepare("SELECT count(*) FROM docs"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
}

struct OldTransactions {
    rstd::sync::Mutex<Option<sqlito::Transaction>> old { None() };
    rstd::sync::Mutex<Option<sqlito::Transaction>> current { None() };
    bool                                           old_last {};
};
auto transaction_factory(rstd::sync::Arc<OldTransactions> resources) -> sqlito::FtsFactory {
    return sqlito::FtsFactory::make(
        [resources = rstd::move(resources)](slice<sqlito::FtsArgument>) noexcept
            -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [resources = resources.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    if (resources->old_last) {
                        {
                            auto value = resources->current.lock().unwrap();
                            *value     = None();
                        }
                        {
                            auto value = resources->old.lock().unwrap();
                            *value     = None();
                        }
                    } else {
                        {
                            auto value = resources->old.lock().unwrap();
                            *value     = None();
                        }
                        {
                            auto value = resources->current.lock().unwrap();
                            *value     = None();
                        }
                    }
                    if (input.text().is_empty()) return Ok(empty {});
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
}
TEST(Fts, StaleTransactionCannotOverrideCurrentRollback) {
    for (bool old_last : { false, true }) {
        auto db             = sqlito::Connection::open(":memory:"_str).unwrap();
        auto resources      = rstd::sync::Arc<OldTransactions>::make();
        resources->old_last = old_last;
        auto registration =
            db.register_or_replace_fts5_tokenizer(String::make("dropper"_str),
                                                  transaction_factory(resources.clone()))
                .unwrap();
        ASSERT_TRUE(
            db.execute_batch("CREATE TABLE t(v INTEGER);"
                             "CREATE TRIGGER fail_t BEFORE INSERT ON t BEGIN SELECT "
                             "RAISE(ROLLBACK,'rollback'); END;"
                             "CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='dropper')"_str)
                .is_ok());
        auto old  = db.begin().unwrap();
        auto fail = old.prepare("INSERT INTO t VALUES(1)"_str).unwrap();
        ASSERT_TRUE(fail.step().is_err());
        auto current = db.begin().unwrap();
        auto insert  = current.prepare("INSERT INTO docs VALUES('hello')"_str).unwrap();
        {
            auto value = resources->old.lock().unwrap();
            *value     = Some(rstd::move(old));
        }
        {
            auto value = resources->current.lock().unwrap();
            *value     = Some(rstd::move(current));
        }
        auto result = insert.step();
        ASSERT_TRUE(result.is_err());
        EXPECT_EQ(result.unwrap_err().kind(), sqlito::ErrorKind::State);
        auto count = db.prepare("SELECT count(*) FROM docs"_str).unwrap();
        ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
    }
}

struct CallbackOwner {
    rstd::sync::Mutex<Option<sqlito::FtsRegistration>> owner { None() };
    rstd::sync::Mutex<Option<sqlito::Statement>>       victim { None() };
};
TEST(Fts, OwnerReleasedInsideCallbackKeepsLeaseUntilReturn) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto shared = rstd::sync::Arc<CallbackOwner>::make();
    auto victim = db.prepare("SELECT 1"_str).unwrap();
    {
        auto value = shared->victim.lock().unwrap();
        *value     = Some(rstd::move(victim));
    }
    auto factory =
        sqlito::FtsFactory::make([shared = shared.clone()](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [shared = shared.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    {
                        auto owner = shared->owner.lock().unwrap();
                        *owner     = None();
                    }
                    {
                        auto victim = shared->victim.lock().unwrap();
                        *victim     = None();
                    }
                    if (input.text().is_empty()) return Ok(empty {});
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
    auto owner =
        db.register_or_replace_fts5_tokenizer(String::make("dropper"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='dropper')"_str)
            .is_ok());
    {
        auto value = shared->owner.lock().unwrap();
        *value     = Some(rstd::move(owner));
    }
    ASSERT_TRUE(db.execute_batch("INSERT INTO docs VALUES('hello')"_str).is_ok());
    EXPECT_TRUE(db.execute_batch("INSERT INTO docs VALUES('expired')"_str).is_err());
    EXPECT_TRUE(db.idle());
}

TEST(Fts, SqliteAndCallbackErrorsSurviveDeferredRollback) {
    auto db        = sqlito::Connection::open(":memory:"_str).unwrap();
    auto resources = rstd::sync::Arc<DropResources>::make();
    auto factory   = sqlito::FtsFactory::make(
        [resources = resources.clone()](slice<sqlito::FtsArgument>) noexcept
            -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [resources = resources.clone()](
                    const sqlito::FtsInput&,
                    sqlito::FtsTokenSink&) noexcept -> Result<empty, sqlito::FtsFailure> {
                    {
                        auto tx = resources->transaction.lock().unwrap();
                        *tx     = None();
                    }
                    return Err(sqlito::FtsFailure {
                        sqlito::ErrorKind::Input, 77, String::make("token failure"_str) });
                }));
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("failure"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='failure')"_str)
            .is_ok());
    auto tx     = db.begin().unwrap();
    auto insert = tx.prepare("INSERT INTO docs VALUES('hello')"_str).unwrap();
    {
        auto value = resources->transaction.lock().unwrap();
        *value     = Some(rstd::move(tx));
    }
    auto result = insert.step();
    ASSERT_TRUE(result.is_err());
    auto failure = result.unwrap_err();
    EXPECT_EQ(failure.kind(), sqlito::ErrorKind::Database);
    ASSERT_TRUE(failure.callback().is_some());
    EXPECT_EQ(failure.callback()->kind, sqlito::ErrorKind::Input);
    EXPECT_EQ(failure.callback()->code, 77);
    EXPECT_EQ(failure.callback()->message.as_str(), "token failure"_str);
    auto count = db.prepare("SELECT count(*) FROM docs"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
}

struct ThreadStart {
    struct State {
        bool started {}, released {};
    };
    rstd::sync::Mutex<State> state { State {} };
    rstd::sync::Condvar      changed;
};
TEST(Fts, ExternalOwnerReleaseDefersCapturedStatementAcrossThreads) {
    auto db      = sqlito::Connection::open(":memory:"_str).unwrap();
    auto started = rstd::sync::Arc<ThreadStart>::make();
    auto blocker_factory =
        sqlito::FtsFactory::make([started = started.clone()](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [started = started.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    auto state     = started->state.lock().unwrap();
                    state->started = true;
                    started->changed.notify_all();
                    started->changed.wait_while(state, [](const ThreadStart::State& value) {
                        return ! value.released;
                    });
                    if (input.text().is_empty()) return Ok(empty {});
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
    auto blocker_owner = db.register_or_replace_fts5_tokenizer(String::make("blocker"_str),
                                                               rstd::move(blocker_factory))
                             .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='blocker')"_str)
            .is_ok());
    auto victim  = db.prepare("SELECT 1"_str).unwrap();
    auto factory = sqlito::FtsFactory::make(
        [victim = rstd::move(victim)](slice<sqlito::FtsArgument>) mutable noexcept
            -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [](const sqlito::FtsInput& input,
                   sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    if (input.text().is_empty()) return Ok(empty {});
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
    Option<sqlito::FtsRegistration> owner =
        Some(db.register_or_replace_fts5_tokenizer(String::make("whole"_str), rstd::move(factory))
                 .unwrap());
    auto query  = db.prepare("INSERT INTO docs VALUES('hello')"_str).unwrap();
    auto worker = rstd::thread::spawn([query = rstd::move(query)]() mutable {
                      return query.step();
                  }).unwrap();
    {
        auto state = started->state.lock().unwrap();
        started->changed.wait_while(state, [](const ThreadStart::State& value) {
            return ! value.started;
        });
    }
    owner = None();
    {
        auto state      = started->state.lock().unwrap();
        state->released = true;
        started->changed.notify_all();
    }
    auto result = rstd::move(worker).join().unwrap();
    ASSERT_TRUE(result.is_ok());
    EXPECT_EQ(result.unwrap(), sqlito::Step::Done);
    EXPECT_TRUE(db.idle());
    EXPECT_TRUE(db.close().is_ok());
}

struct Modes {
    usize documents {}, queries {}, prefixes {};
    bool  embedded_nul {}, invalid_utf8 {};
};
TEST(Fts, ModesAndBoundedInputBytes) {
    auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
    auto modes = rstd::sync::Arc<Modes>::make();
    auto factory =
        sqlito::FtsFactory::make([modes = modes.clone()](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [modes = modes.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    if (input.mode() == sqlito::FtsMode::Document) ++modes->documents;
                    if (input.mode() == sqlito::FtsMode::Query) ++modes->queries;
                    if (input.mode() == sqlito::FtsMode::QueryPrefix) ++modes->prefixes;
                    auto bytes = input.text();
                    if (bytes.len() == usize(3) && bytes[usize(1)] == u8())
                        modes->embedded_nul = true;
                    if (bytes.len() == usize(1) && bytes[usize()] == u8(128))
                        modes->invalid_utf8 = true;
                    if (bytes.is_empty()) return Ok(empty {});
                    return sink.emit(bytes, usize(), bytes.len());
                }));
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("whole"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='whole');"
                                 "INSERT INTO docs VALUES('alpha');"_str)
                    .is_ok());
    auto insert = db.prepare("INSERT INTO docs VALUES(?1)"_str).unwrap();
    ASSERT_TRUE(insert.bind_text(usize(1), "a\0b"_str).is_ok());
    ASSERT_EQ(insert.step().unwrap(), sqlito::Step::Done);
    ASSERT_TRUE(db.execute_batch("INSERT INTO docs VALUES(CAST(X'80' AS TEXT))"_str).is_ok());
    ASSERT_TRUE(db.execute_batch("SELECT rowid FROM docs WHERE docs MATCH 'alpha';"
                                 "SELECT rowid FROM docs WHERE docs MATCH 'alph*';"_str)
                    .is_ok());
    EXPECT_TRUE(modes->documents >= usize(3));
    EXPECT_TRUE(modes->queries >= usize(1));
    EXPECT_TRUE(modes->prefixes >= usize(1));
    EXPECT_TRUE(modes->embedded_nul);
    EXPECT_TRUE(modes->invalid_utf8);
}

TEST(Fts, IgnoredInvalidTokenStillStopsOperation) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    auto factory =
        sqlito::FtsFactory::make([](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [](const sqlito::FtsInput& input,
                   sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    (void)sink.emit(input.text(), usize(1), usize());
                    return Ok(empty {});
                }));
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("invalid"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='invalid')"_str)
            .is_ok());
    auto result = db.execute_batch("INSERT INTO docs VALUES('x')"_str);
    ASSERT_TRUE(result.is_err());
    auto failure = result.unwrap_err();
    ASSERT_TRUE(failure.callback().is_some());
    EXPECT_EQ(failure.callback()->kind, sqlito::ErrorKind::Input);
}

TEST(Fts, TokenRangesMustBeInInputOrder) {
    for (bool reversed : { false, true }) {
        auto db = sqlito::Connection::open(":memory:"_str).unwrap();
        auto factory =
            sqlito::FtsFactory::make([reversed](slice<sqlito::FtsArgument>) noexcept
                                         -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
                return Ok(sqlito::FtsTokenizer::make(
                    [reversed](const sqlito::FtsInput& input, sqlito::FtsTokenSink& sink) noexcept
                        -> Result<empty, sqlito::FtsFailure> {
                        if (reversed) {
                            (void)sink.emit(input.text(), usize(2), usize(3));
                            (void)sink.emit(input.text(), usize(1), usize(2));
                            return Ok(empty {});
                        }
                        auto first = sink.emit(input.text(), usize(), usize(2));
                        if (first.is_err()) return first;
                        auto synonym = sink.emit(input.text(), usize(), usize(2), true);
                        if (synonym.is_err()) return synonym;
                        return sink.emit(input.text(), usize(1), usize(3));
                    }));
            });
        auto registration =
            db.register_or_replace_fts5_tokenizer(String::make("ranges"_str), rstd::move(factory))
                .unwrap();
        ASSERT_TRUE(
            db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='ranges')"_str)
                .is_ok());
        auto result = db.execute_batch("INSERT INTO docs VALUES('abc')"_str);
        if (reversed) {
            ASSERT_TRUE(result.is_err());
            auto failure = result.unwrap_err();
            ASSERT_TRUE(failure.callback().is_some());
            EXPECT_EQ(failure.callback()->kind, sqlito::ErrorKind::Input);
        } else {
            EXPECT_TRUE(result.is_ok());
        }
    }
}

TEST(Fts, ZeroLengthTokenIsPassedToSqlite) {
    auto db = sqlito::Connection::open(":memory:"_str).unwrap();
    auto factory =
        sqlito::FtsFactory::make([](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [](const sqlito::FtsInput&,
                   sqlito::FtsTokenSink& sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    return sink.emit((""_str).as_bytes(), usize(), usize());
                }));
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("empty"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='empty')"_str)
                    .is_ok());
    EXPECT_TRUE(db.execute_batch("INSERT INTO docs VALUES('x')"_str).is_ok());
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE vocab USING fts5vocab(docs, 'row')"_str).is_ok());
    auto vocabulary = db.prepare("SELECT count(*), min(term IS NULL) FROM vocab"_str).unwrap();
    ASSERT_EQ(vocabulary.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*vocabulary.integer(usize()).unwrap(), i64(1));
    EXPECT_EQ(*vocabulary.integer(usize(1)).unwrap(), i64(1));
}

TEST(Fts, CancellationFromTokenizerPropagates) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto cancel = sqlito::Cancellation::make();
    auto factory =
        sqlito::FtsFactory::make([cancel = cancel.clone()](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Ok(sqlito::FtsTokenizer::make(
                [cancel = cancel.clone()](
                    const sqlito::FtsInput& input,
                    sqlito::FtsTokenSink&   sink) noexcept -> Result<empty, sqlito::FtsFailure> {
                    cancel.cancel();
                    return sink.emit(input.text(), usize(), input.text().len());
                }));
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("cancel"_str), rstd::move(factory))
            .unwrap();
    ASSERT_TRUE(
        db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='cancel')"_str)
            .is_ok());
    auto result = db.run(cancel, [](auto& session) -> Result<empty, sqlito::Error> {
        return session.execute_batch("INSERT INTO docs VALUES('x')"_str);
    });
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind(), sqlito::ErrorKind::Cancelled);
}

TEST(Fts, FactoryErrorDoesNotLeakToNextOperation) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = rstd::path::PathBuf::from(directory.path()).join("prepare.sqlite"_str);
    auto name      = path.as_path().to_str().unwrap();
    {
        auto db = sqlito::Connection::open(name).unwrap();
        auto registration =
            db.register_or_replace_fts5_tokenizer(String::make("whole"_str), simple_factory())
                .unwrap();
        ASSERT_TRUE(
            db.execute_batch("CREATE VIRTUAL TABLE docs USING fts5(text, tokenize='whole')"_str)
                .is_ok());
    }
    auto db = sqlito::Connection::open(name).unwrap();
    auto factory =
        sqlito::FtsFactory::make([](slice<sqlito::FtsArgument>) noexcept
                                     -> Result<sqlito::FtsTokenizer, sqlito::FtsFailure> {
            return Err(sqlito::FtsFailure {
                sqlito::ErrorKind::Input, 23, String::make("factory failed"_str) });
        });
    auto registration =
        db.register_or_replace_fts5_tokenizer(String::make("whole"_str), rstd::move(factory))
            .unwrap();
    auto bad = db.prepare("SELECT * FROM docs WHERE docs MATCH 'x'"_str);
    auto failure =
        bad.is_err() ? rstd::move(bad).unwrap_err() : rstd::move(bad).unwrap().step().unwrap_err();
    ASSERT_TRUE(failure.callback().is_some());
    EXPECT_EQ(failure.callback()->code, 23);
    auto good = db.prepare("SELECT 1"_str).unwrap();
    ASSERT_EQ(good.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*good.integer(usize()).unwrap(), i64(1));
}

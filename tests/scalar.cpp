#include <rstd/test/gtest.hpp>
#include <rstd/macro.hpp>
import sqlito;
using namespace rstd::prelude;
using namespace rstd::literals;

struct SeenScalar {
    bool null_value {}, empty_value {}, embedded_nul {}, invalid_utf8 {}, converted_integer {};
};
auto observing_scalar(rstd::sync::Arc<SeenScalar> seen) -> sqlito::ScalarFunction {
    return sqlito::ScalarFunction::make(
        [seen = rstd::move(seen)](const sqlito::ScalarInput& input) noexcept
            -> Result<Option<String>, sqlito::CallbackFailure> {
            auto value = input.value();
            if (value.is_none()) {
                seen->null_value = true;
                return Ok<Option<String>>(None());
            }
            auto bytes = *value;
            if (bytes.is_empty()) {
                seen->empty_value = true;
                return Ok(Some(String::make(""_str)));
            }
            if (bytes.len() == usize(3) && bytes[usize(1)] == u8()) seen->embedded_nul = true;
            if (bytes.len() == usize(3) && bytes[usize()] == u8('1'))
                seen->converted_integer = true;
            auto text = rstd::str_::from_utf8(bytes);
            if (text.is_err()) {
                seen->invalid_utf8 = true;
                return Ok(Some(String::make("invalid"_str)));
            }
            return Ok(Some(String::make(*text)));
        });
}

TEST(Scalar, BoundedValuesNullAndOwnedResult) {
    auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
    auto seen  = rstd::sync::Arc<SeenScalar>::make();
    auto owner = db.register_or_replace_scalar_function(
                       String::make("observe"_str), observing_scalar(seen.clone()), true)
                     .unwrap();
    auto stmt =
        db.prepare(
              "SELECT observe(NULL), observe(''), observe(?1), observe(CAST(X'80' AS TEXT)), observe(123), observe(X'')"_str)
            .unwrap();
    ASSERT_TRUE(stmt.bind_text(usize(1), "a\0b"_str).is_ok());
    ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
    EXPECT_TRUE(stmt.text(usize()).unwrap().is_none());
    EXPECT_EQ(stmt.text(usize(1)).unwrap()->as_str(), ""_str);
    EXPECT_EQ(stmt.text(usize(2)).unwrap()->as_str(), "a\0b"_str);
    EXPECT_EQ(stmt.text(usize(3)).unwrap()->as_str(), "invalid"_str);
    EXPECT_EQ(stmt.text(usize(4)).unwrap()->as_str(), "123"_str);
    EXPECT_EQ(stmt.text(usize(5)).unwrap()->as_str(), ""_str);
    EXPECT_TRUE(seen->null_value);
    EXPECT_TRUE(seen->empty_value);
    EXPECT_TRUE(seen->embedded_nul);
    EXPECT_TRUE(seen->invalid_utf8);
    EXPECT_TRUE(seen->converted_integer);
    ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Done);
}

TEST(Scalar, DeterministicFlagAndExpiredOwner) {
    auto                               db   = sqlito::Connection::open(":memory:"_str).unwrap();
    auto                               seen = rstd::sync::Arc<SeenScalar>::make();
    Option<sqlito::ScalarRegistration> owner =
        Some(db.register_or_replace_scalar_function(
                   String::make("observe"_str), observing_scalar(seen.clone()), true)
                 .unwrap());
    ASSERT_TRUE(
        db.execute_batch(
              "CREATE TABLE items(v TEXT); CREATE INDEX by_observation ON items(observe(v));"_str)
            .is_ok());
    auto stmt   = db.prepare("SELECT observe('x')"_str).unwrap();
    owner       = None();
    auto result = stmt.step();
    ASSERT_TRUE(result.is_err());
    auto failure = result.unwrap_err();
    ASSERT_TRUE(failure.callback.is_some());
    EXPECT_EQ(failure.callback->kind, sqlito::ErrorKind::State);
    EXPECT_EQ(failure.message.as_str(), "scalar registration expired"_str);

    auto nondeterministic = db.register_or_replace_scalar_function(
                                  String::make("other"_str), observing_scalar(seen.clone()), false)
                                .unwrap();
    EXPECT_TRUE(db.execute_batch("CREATE INDEX by_other ON items(other(v))"_str).is_err());
}

TEST(Scalar, ReplacementAndActiveStatement) {
    auto db   = sqlito::Connection::open(":memory:"_str).unwrap();
    auto seen = rstd::sync::Arc<SeenScalar>::make();
    auto old  = db.register_or_replace_scalar_function(String::make("observe"_str),
                                                       observing_scalar(seen.clone()))
                    .unwrap();
    {
        auto stmt   = db.prepare("SELECT observe('first')"_str).unwrap();
        auto victim = db.prepare("SELECT 1"_str).unwrap();
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
        auto captures_statement = sqlito::ScalarFunction::make(
            [victim = rstd::move(victim)](const sqlito::ScalarInput&) mutable noexcept
                -> Result<Option<String>, sqlito::CallbackFailure> {
                return Ok(Some(String::make("unused"_str)));
            });
        auto busy = db.register_or_replace_scalar_function(String::make("observe"_str),
                                                           rstd::move(captures_statement));
        ASSERT_TRUE(busy.is_err());
        ASSERT_TRUE(stmt.reset().is_ok());
        auto newer = db.register_or_replace_scalar_function(String::make("observe"_str),
                                                            observing_scalar(seen.clone()))
                         .unwrap();
        ASSERT_EQ(stmt.step().unwrap(), sqlito::Step::Row);
        EXPECT_EQ(stmt.text(usize()).unwrap()->as_str(), "first"_str);
    }
    EXPECT_TRUE(db.idle());
}

struct ScalarDropOnce {
    int* count;
    explicit ScalarDropOnce(int& value): count(&value) {}
    ScalarDropOnce(const ScalarDropOnce&) = delete;
    ScalarDropOnce(ScalarDropOnce&& other) noexcept: count(rstd::exchange(other.count, nullptr)) {}
    ~ScalarDropOnce() {
        if (count) ++*count;
    }
};
TEST(Scalar, InvalidLongNameReleasesCapturedResourcesOnce) {
    auto db       = sqlito::Connection::open(":memory:"_str).unwrap();
    int  dropped  = 0;
    auto victim   = db.prepare("SELECT 1"_str).unwrap();
    auto function = sqlito::ScalarFunction::make(
        [resource = ScalarDropOnce(dropped),
         victim   = rstd::move(victim)](const sqlito::ScalarInput&) mutable noexcept
            -> Result<Option<String>, sqlito::CallbackFailure> {
            return Ok(Some(String::make("unused"_str)));
        });
    auto result = db.register_or_replace_scalar_function(
        String::make(
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"_str),
        rstd::move(function));
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(dropped, 1);
    EXPECT_TRUE(db.idle());
    auto check = db.prepare("SELECT 42"_str).unwrap();
    ASSERT_EQ(check.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*check.integer(usize()).unwrap(), i64(42));
    EXPECT_EQ(dropped, 1);
}

struct ScalarDrop {
    rstd::sync::Mutex<Option<sqlito::Statement>>   statement { None() };
    rstd::sync::Mutex<Option<sqlito::Transaction>> transaction { None() };
};
TEST(Scalar, CallbackReleaseDefersResourcesAndRollsBack) {
    auto db    = sqlito::Connection::open(":memory:"_str).unwrap();
    auto state = rstd::sync::Arc<ScalarDrop>::make();
    auto function =
        sqlito::ScalarFunction::make([state = state.clone()](const sqlito::ScalarInput&) noexcept
                                         -> Result<Option<String>, sqlito::CallbackFailure> {
            {
                auto value = state->statement.lock().unwrap();
                *value     = None();
            }
            {
                auto value = state->transaction.lock().unwrap();
                *value     = None();
            }
            return Ok(Some(String::make("value"_str)));
        });
    auto owner =
        db.register_or_replace_scalar_function(String::make("dropper"_str), rstd::move(function))
            .unwrap();
    ASSERT_TRUE(db.execute_batch("CREATE TABLE items(v TEXT)"_str).is_ok());
    auto tx = db.begin().unwrap();
    ASSERT_TRUE(tx.execute_batch("INSERT INTO items VALUES('written')"_str).is_ok());
    auto victim = tx.prepare("SELECT 1"_str).unwrap();
    auto stmt   = tx.prepare("SELECT dropper('x')"_str).unwrap();
    {
        auto value = state->statement.lock().unwrap();
        *value     = Some(rstd::move(victim));
    }
    {
        auto value = state->transaction.lock().unwrap();
        *value     = Some(rstd::move(tx));
    }
    auto result = stmt.step();
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::State);
    auto count = db.prepare("SELECT count(*) FROM items"_str).unwrap();
    ASSERT_EQ(count.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(*count.integer(usize()).unwrap(), i64());
}

TEST(Scalar, CallbackErrorAndCancellation) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto failed = sqlito::ScalarFunction::make(
        [](const sqlito::ScalarInput&) noexcept -> Result<Option<String>, sqlito::CallbackFailure> {
            return Err(sqlito::CallbackFailure {
                sqlito::ErrorKind::Input, 29, String::make("invalid scalar"_str) });
        });
    auto failure_owner =
        db.register_or_replace_scalar_function(String::make("failure"_str), rstd::move(failed))
            .unwrap();
    auto bad            = db.prepare("SELECT failure('x')"_str).unwrap();
    auto failure_result = bad.step();
    ASSERT_TRUE(failure_result.is_err());
    auto failure = failure_result.unwrap_err();
    ASSERT_TRUE(failure.callback.is_some());
    EXPECT_EQ(failure.callback->kind, sqlito::ErrorKind::Input);
    EXPECT_EQ(failure.callback->code, 29);
    EXPECT_EQ(failure.callback->message.as_str(), "invalid scalar"_str);
    EXPECT_EQ(failure.message.as_str(), "invalid scalar"_str);

    auto cancelled_failure = sqlito::ScalarFunction::make(
        [](const sqlito::ScalarInput&) noexcept -> Result<Option<String>, sqlito::CallbackFailure> {
            return Err(sqlito::CallbackFailure {
                sqlito::ErrorKind::Cancelled, 17, String::make("取消"_str) });
        });
    auto cancelled_owner =
        db.register_or_replace_scalar_function(String::make("cancelled_failure"_str),
                                               rstd::move(cancelled_failure))
            .unwrap();
    auto cancelled_stmt   = db.prepare("SELECT cancelled_failure('x')"_str).unwrap();
    auto cancelled_result = cancelled_stmt.step();
    ASSERT_TRUE(cancelled_result.is_err());
    auto cancelled_error = cancelled_result.unwrap_err();
    EXPECT_EQ(cancelled_error.kind, sqlito::ErrorKind::Cancelled);
    EXPECT_EQ(cancelled_error.message.as_str(), "取消"_str);
    ASSERT_TRUE(cancelled_error.callback.is_some());
    EXPECT_EQ(cancelled_error.callback->code, 17);

    auto cancel = sqlito::Cancellation::make();
    auto cancelled =
        sqlito::ScalarFunction::make([cancel = cancel.clone()](const sqlito::ScalarInput&) noexcept
                                         -> Result<Option<String>, sqlito::CallbackFailure> {
            cancel.cancel();
            return Ok(Some(String::make("x"_str)));
        });
    auto cancel_owner =
        db.register_or_replace_scalar_function(String::make("cancel"_str), rstd::move(cancelled))
            .unwrap();
    auto result = db.run(cancel, [](auto& session) -> Result<empty, sqlito::Error> {
        auto stmt = rstd_try(session.prepare("SELECT cancel('x')"_str));
        rstd_try(stmt.step());
        return Ok(empty {});
    });
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind, sqlito::ErrorKind::Cancelled);
}

struct ScalarGate {
    struct State {
        bool started {}, released {};
    };
    rstd::sync::Mutex<State> state { State {} };
    rstd::sync::Condvar      changed;
};
TEST(Scalar, OtherThreadDropsOwnerWhileScalarIsRunning) {
    auto db   = sqlito::Connection::open(":memory:"_str).unwrap();
    auto gate = rstd::sync::Arc<ScalarGate>::make();
    auto blocking =
        sqlito::ScalarFunction::make([gate = gate.clone()](const sqlito::ScalarInput&) noexcept
                                         -> Result<Option<String>, sqlito::CallbackFailure> {
            auto state     = gate->state.lock().unwrap();
            state->started = true;
            gate->changed.notify_all();
            gate->changed.wait_while(state, [](const ScalarGate::State& value) {
                return ! value.released;
            });
            return Ok(Some(String::make("done"_str)));
        });
    auto blocking_owner =
        db.register_or_replace_scalar_function(String::make("blocking"_str), rstd::move(blocking))
            .unwrap();
    auto victim             = db.prepare("SELECT 1"_str).unwrap();
    auto captures_statement = sqlito::ScalarFunction::make(
        [victim = rstd::move(victim)](const sqlito::ScalarInput&) mutable noexcept
            -> Result<Option<String>, sqlito::CallbackFailure> {
            return Ok(Some(String::make("unused"_str)));
        });
    Option<sqlito::ScalarRegistration> owner =
        Some(db.register_or_replace_scalar_function(String::make("held"_str),
                                                    rstd::move(captures_statement))
                 .unwrap());
    auto query  = db.prepare("SELECT blocking('x')"_str).unwrap();
    auto worker = rstd::thread::spawn([query = rstd::move(query)]() mutable {
                      return query.step();
                  }).unwrap();
    {
        auto state = gate->state.lock().unwrap();
        gate->changed.wait_while(state, [](const ScalarGate::State& value) {
            return ! value.started;
        });
    }
    owner = None();
    {
        auto state      = gate->state.lock().unwrap();
        state->released = true;
        gate->changed.notify_all();
    }
    auto result = rstd::move(worker).join().unwrap();
    ASSERT_TRUE(result.is_ok());
    EXPECT_EQ(result.unwrap(), sqlito::Step::Row);
    EXPECT_TRUE(db.idle());
    EXPECT_TRUE(db.close().is_ok());
}

struct ScalarOwnedState {
    rstd::sync::Mutex<Option<sqlito::ScalarRegistration>> owner { None() };
    rstd::sync::Mutex<Option<sqlito::Statement>>          victim { None() };
};
TEST(Scalar, OwnerReleasedDuringOwnCallbackKeepsLease) {
    auto db     = sqlito::Connection::open(":memory:"_str).unwrap();
    auto shared = rstd::sync::Arc<ScalarOwnedState>::make();
    {
        auto value = shared->victim.lock().unwrap();
        *value     = Some(db.prepare("SELECT 1"_str).unwrap());
    }
    auto function =
        sqlito::ScalarFunction::make([shared = shared.clone()](const sqlito::ScalarInput&) noexcept
                                         -> Result<Option<String>, sqlito::CallbackFailure> {
            {
                auto value = shared->owner.lock().unwrap();
                *value     = None();
            }
            {
                auto value = shared->victim.lock().unwrap();
                *value     = None();
            }
            return Ok(Some(String::make("done"_str)));
        });
    auto owner =
        db.register_or_replace_scalar_function(String::make("release"_str), rstd::move(function))
            .unwrap();
    {
        auto value = shared->owner.lock().unwrap();
        *value     = Some(rstd::move(owner));
    }
    auto query = db.prepare("SELECT release('x')"_str).unwrap();
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Row);
    EXPECT_EQ(query.text(usize()).unwrap()->as_str(), "done"_str);
    ASSERT_EQ(query.step().unwrap(), sqlito::Step::Done);
    ASSERT_TRUE(query.reset().is_ok());
    EXPECT_TRUE(query.step().is_err());
}

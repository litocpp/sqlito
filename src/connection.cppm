module;
#include <sqlite3.h>
#include <rstd/macro.hpp>
#include <climits>
#include <cstring>
export module sqlito:connection;
export import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::sync::Arc;
using AtomicBool = rstd::sync::atomic::Atomic<bool>;
using Ordering   = rstd::sync::atomic::Ordering;

export namespace sqlito
{
enum class ErrorKind
{
    Database,
    State,
    Input,
    Unbound,
    Type,
    Range,
    Cancelled,
    Busy
};
struct CallbackFailure {
    ErrorKind kind;
    int       code {};
    String    message;
};
using FtsFailure = CallbackFailure;
struct Error {
    ErrorKind               kind;
    int                     code;
    String                  message;
    Option<CallbackFailure> callback;
};
enum class Step
{
    Row,
    Done
};
enum class ColumnType
{
    Null,
    Integer,
    Real,
    Text,
    Blob
};
enum class TransactionMode
{
    Deferred,
    Immediate,
    Exclusive
};
class Cancellation {
    Arc<AtomicBool> state_;
    explicit Cancellation(Arc<AtomicBool> state): state_(rstd::move(state)) {}

public:
    static auto make() -> Cancellation { return Cancellation { Arc<AtomicBool>::make(false) }; }
    auto        clone() const -> Cancellation { return Cancellation { state_.clone() }; }
    void        cancel() const { state_->store(true, Ordering::Release); }
    auto        cancelled() const -> bool { return state_->load(Ordering::Acquire); }
};
enum class FtsMode
{
    Document,
    Query,
    QueryPrefix,
    Auxiliary
};
// Argument and input views are valid only during their synchronous callback.
struct FtsArgument {
    slice<u8> bytes;
};
class FtsTokenSink;
struct FtsAdapter;
class FtsInput {
    friend struct FtsAdapter;
    slice<u8>           text_;
    FtsMode             mode_;
    Option<slice<u8>>   locale_;
    const Cancellation* cancel_;
    FtsInput(slice<u8> text, FtsMode mode, Option<slice<u8>> locale, const Cancellation* cancel)
        : text_(text), mode_(mode), locale_(locale), cancel_(cancel) {}

public:
    FtsInput(const FtsInput&)                    = delete;
    auto operator=(const FtsInput&) -> FtsInput& = delete;
    auto text() const noexcept -> slice<u8> { return text_; }
    auto mode() const noexcept -> FtsMode { return mode_; }
    auto locale() const noexcept -> Option<slice<u8>> { return locale_; }
    auto cancelled() const noexcept -> bool { return cancel_ && cancel_->cancelled(); }
};
using FtsTokenizer = alloc::boxed::Box<
    dyn<FnMut<Result<empty, FtsFailure>(const FtsInput&, FtsTokenSink&) noexcept>>>;
using FtsFactory =
    alloc::boxed::Box<dyn<FnMut<Result<FtsTokenizer, FtsFailure>(slice<FtsArgument>) noexcept>>>;
struct ScalarAdapter;
class ScalarInput {
    friend struct ScalarAdapter;
    Option<slice<u8>>   value_;
    const Cancellation* cancel_;
    ScalarInput(Option<slice<u8>> value, const Cancellation* cancel)
        : value_(value), cancel_(cancel) {}

public:
    ScalarInput(const ScalarInput&)                    = delete;
    auto operator=(const ScalarInput&) -> ScalarInput& = delete;
    // SQL NULL is None; other values are SQLite's bounded text bytes. The
    // returned view is valid only during this synchronous callback.
    auto value() const noexcept -> Option<slice<u8>> { return value_; }
    auto cancelled() const noexcept -> bool { return cancel_ && cancel_->cancelled(); }
};
using ScalarFunction = alloc::boxed::Box<
    dyn<FnMut<Result<Option<String>, CallbackFailure>(const ScalarInput&) noexcept>>>;
} // namespace sqlito
namespace sqlito
{
struct FtsTokenOwner {
    FtsTokenizer tokenizer;
};
struct FtsOwner {
    FtsFactory                      factory;
    Vec<Option<Arc<FtsTokenOwner>>> instances;
    explicit FtsOwner(FtsFactory value): factory(rstd::move(value)) {}
};
struct ScalarOwner {
    ScalarFunction function;
    explicit ScalarOwner(ScalarFunction value): function(rstd::move(value)) {}
};
} // namespace sqlito
export namespace sqlito
{
class Connection;
class FtsRegistration {
    friend class Connection;
    Arc<FtsOwner> owner_;
    explicit FtsRegistration(Arc<FtsOwner> owner): owner_(rstd::move(owner)) {}

public:
    // SQLite keeps weak references. A callback already running keeps a lease;
    // subsequent callbacks return an error after the registration is released.
    FtsRegistration(const FtsRegistration&)                        = delete;
    auto operator=(const FtsRegistration&) -> FtsRegistration&     = delete;
    FtsRegistration(FtsRegistration&&) noexcept                    = default;
    auto operator=(FtsRegistration&&) noexcept -> FtsRegistration& = default;
};
class ScalarRegistration {
    friend class Connection;
    Arc<ScalarOwner> owner_;
    explicit ScalarRegistration(Arc<ScalarOwner> owner): owner_(rstd::move(owner)) {}

public:
    // SQLite retains a weak reference; callbacks in progress keep a lease.
    ScalarRegistration(const ScalarRegistration&)                        = delete;
    auto operator=(const ScalarRegistration&) -> ScalarRegistration&     = delete;
    ScalarRegistration(ScalarRegistration&&) noexcept                    = default;
    auto operator=(ScalarRegistration&&) noexcept -> ScalarRegistration& = default;
};
} // namespace sqlito
namespace sqlito
{
auto error(ErrorKind kind, ref<str> message) -> Error {
    return { kind, 0, String::make(message) };
}
auto database_error(sqlite3* db, int code) -> Error {
    auto bytes =
        rstd::ffi::CStr::from_ptr(db ? sqlite3_errmsg(db) : sqlite3_errstr(code)).to_bytes();
    auto text = rstd::str_::from_utf8(bytes);
    return { (code & 255) == SQLITE_INTERRUPT ? ErrorKind::Cancelled : ErrorKind::Database,
             code,
             String::make(text.is_ok() ? *text : "SQLite error"_str) };
}
auto terminated(ref<str> input) -> Result<Vec<u8>, Error> {
    for (auto byte : input.as_bytes())
        if (byte == u8()) return Err(error(ErrorKind::Input, "embedded NUL"_str));
    auto bytes = Vec<u8>::from(input.as_bytes());
    bytes.push(u8());
    return Ok(rstd::move(bytes));
}
struct Database;
auto database_error(Database& db, int code) -> Error;
struct Database {
    sqlite3*           handle;
    unsigned long long active {}, generation {};
    unsigned long long statements {};
    bool               control_allowed {}, poisoned {};
    struct StatementNode {
        sqlite3_stmt*  statement {};
        StatementNode* next {};
    };
    struct Gate {
        bool               busy {};
        unsigned long long pending_rollback {};
        StatementNode*     deferred {};
    };
    rstd::sync::Mutex<Gate> gate { Gate {} };
    rstd::sync::Condvar     gate_changed;
    Option<Cancellation>    cancellation;
    Option<CallbackFailure> callback_error;
    explicit Database(sqlite3* value): handle(value) {}
    auto cancelled() const -> bool { return cancellation.is_some() && cancellation->cancelled(); }
    auto valid(unsigned long long token) -> bool {
        if (! handle || poisoned) return false;
        if (active && sqlite3_get_autocommit(handle)) active = 0;
        return active == token;
    }
    auto control(const char* sql) -> Result<empty, Error> {
        control_allowed = true;
        callback_error  = None();
        int code        = sqlite3_exec(handle, sql, nullptr, nullptr, nullptr);
        control_allowed = false;
        if (code != SQLITE_OK) return Err(database_error(*this, code));
        callback_error = None();
        return Ok(empty {});
    }
    auto enter() -> bool {
        auto state = gate.lock().unwrap();
        if (state->busy) return false;
        state->busy = true;
        return true;
    }
    void enter_wait() {
        auto state = gate.lock().unwrap();
        gate_changed.wait_while(state, [](const Gate& value) {
            return value.busy;
        });
        state->busy = true;
    }
    void release_statement(StatementNode* node) {
        if (! node) return;
        bool drain_here = false;
        {
            auto state      = gate.lock().unwrap();
            node->next      = state->deferred;
            state->deferred = node;
            if (! state->busy) {
                state->busy = true;
                drain_here  = true;
            }
        }
        if (drain_here) drain();
    }
    void defer_rollback(unsigned long long token) {
        bool drain_here = false;
        {
            auto state = gate.lock().unwrap();
            if (token > state->pending_rollback) state->pending_rollback = token;
            if (! state->busy) {
                state->busy = true;
                drain_here  = true;
            }
        }
        if (drain_here) drain();
    }
    auto drain() -> bool {
        bool rolled_back = false;
        for (;;) {
            unsigned long long token {};
            StatementNode*     node = nullptr;
            {
                auto state = gate.lock().unwrap();
                token      = rstd::exchange(state->pending_rollback, 0ULL);
                if (! token && state->deferred) {
                    node            = state->deferred;
                    state->deferred = node->next;
                }
                if (! token && ! node) {
                    state->busy = false;
                    gate_changed.notify_all();
                    return rolled_back;
                }
            }
            if (token) {
                if (active == token && ! sqlite3_get_autocommit(handle)) {
                    rolled_back     = true;
                    control_allowed = true;
                    callback_error  = None();
                    int code        = sqlite3_exec(handle, "ROLLBACK", nullptr, nullptr, nullptr);
                    control_allowed = false;
                    if (code != SQLITE_OK) poisoned = true;
                    callback_error = None();
                    if (sqlite3_get_autocommit(handle)) active = 0;
                }
            }
            if (node) {
                callback_error = None();
                (void)sqlite3_finalize(node->statement);
                callback_error = None();
                auto owned     = alloc::boxed::Box<StatementNode>::from_raw(
                    rstd::mut_ptr<StatementNode>::from_raw_parts(node));
                --statements;
            }
        }
    }
    ~Database() {
        if (handle) (void)sqlite3_close_v2(handle);
    }
};
auto database_error(Database& db, int code) -> Error {
    auto result     = database_error(db.handle, code);
    result.callback = db.callback_error.take();
    if (result.callback.is_some() && result.callback->kind == ErrorKind::Cancelled)
        result.kind = ErrorKind::Cancelled;
    return result;
}
struct DatabaseCall {
    Arc<Database> owner;
    Database&     db;
    bool          entered;
    explicit DatabaseCall(const Arc<Database>& value)
        : owner(value.clone()), db(*owner), entered(db.enter()) {}
    auto rollback_pending() const -> bool {
        auto state = db.gate.lock().unwrap();
        if (db.active && sqlite3_get_autocommit(db.handle)) db.active = 0;
        return db.active && state->pending_rollback == db.active;
    }
    auto finish() -> bool {
        if (! entered) return false;
        entered = false;
        return db.drain();
    }
    ~DatabaseCall() {
        if (entered) db.drain();
    }
};
template<class T>
auto complete(DatabaseCall& call, Result<T, Error> value) -> Result<T, Error> {
    if (value.is_err()) return rstd::move(value);
    if (call.finish())
        return Err(error(ErrorKind::State, "transaction released during operation"_str));
    return rstd::move(value);
}
struct FtsContext {
    Database*                  database;
    rstd::sync::Weak<FtsOwner> owner;
};
struct ScalarContext {
    Database*                     database;
    rstd::sync::Weak<ScalarOwner> owner;
};
struct FtsInstance {
    Database*                       database;
    rstd::sync::Weak<FtsOwner>      owner;
    rstd::sync::Weak<FtsTokenOwner> tokenizer;
    usize                           index;
};
} // namespace sqlito
export namespace sqlito
{
class FtsTokenSink {
    friend struct FtsAdapter;
    Database* database_;
    void*     context_;
    int (*emit_)(void*, int, const char*, int, int, int);
    usize              input_size_;
    usize              previous_start_ {};
    bool               emitted_ {};
    Option<FtsFailure> failure_;
    FtsTokenSink(Database& database,
                 void*     context,
                 int (*emit)(void*, int, const char*, int, int, int),
                 usize input_size)
        : database_(&database), context_(context), emit_(emit), input_size_(input_size) {}

public:
    // Offsets are byte positions in the original input, with nondecreasing
    // starts. The first token must not be colocated. Views must not escape.
    FtsTokenSink(const FtsTokenSink&)                    = delete;
    auto operator=(const FtsTokenSink&) -> FtsTokenSink& = delete;
    FtsTokenSink(FtsTokenSink&&)                         = delete;
    auto operator=(FtsTokenSink&&) -> FtsTokenSink&      = delete;
    auto emit(slice<u8> token, usize start, usize end, bool colocated = false) noexcept
        -> Result<empty, FtsFailure>;
};
} // namespace sqlito
namespace sqlito
{
auto copy_fts_failure(const FtsFailure& value) -> FtsFailure {
    return { value.kind, value.code, value.message.clone() };
}
auto FtsTokenSink::emit(slice<u8> token, usize start, usize end, bool colocated) noexcept
    -> Result<empty, FtsFailure> {
    if (failure_.is_some()) return Err(copy_fts_failure(*failure_));
    if (database_->cancelled())
        failure_ = Some(FtsFailure {
            ErrorKind::Cancelled, SQLITE_INTERRUPT, String::make("operation cancelled"_str) });
    else if (start > end || end > input_size_ || token.len() > usize(INT_MAX) ||
             (emitted_ && start < previous_start_) || (colocated && ! emitted_))
        failure_ = Some(FtsFailure {
            ErrorKind::Input, SQLITE_ERROR, String::make("invalid token range or flags"_str) });
    if (failure_.is_some()) return Err(copy_fts_failure(*failure_));
    auto bytes = token.is_empty() ? "" : reinterpret_cast<const char*>(token.as_raw_ptr());
    int  code  = emit_(context_,
                       colocated ? FTS5_TOKEN_COLOCATED : 0,
                       bytes,
                       int(token.len().to_primitive()),
                       int(start.to_primitive()),
                       int(end.to_primitive()));
    if (code != SQLITE_OK) {
        failure_ = Some(FtsFailure {
            ErrorKind::Database, code, String::make("FTS5 token callback failed"_str) });
        return Err(copy_fts_failure(*failure_));
    }
    emitted_        = true;
    previous_start_ = start;
    return Ok(empty {});
}
struct FtsAdapter {
    static auto create(void* raw, const char** args, int count, Fts5Tokenizer** out) noexcept
        -> int {
        auto& context = *static_cast<FtsContext*>(raw);
        auto& db      = *context.database;
        if (db.cancelled()) return SQLITE_INTERRUPT;
        auto owner = context.owner.upgrade();
        if (! owner) {
            db.callback_error = Some(FtsFailure {
                ErrorKind::State, SQLITE_ABORT, String::make("FTS registration expired"_str) });
            return SQLITE_ABORT;
        }
        if (count < 0) return SQLITE_MISUSE;
        auto allocated = Vec<FtsArgument>::try_with_capacity(usize(count));
        if (allocated.is_err()) return SQLITE_NOMEM;
        auto arguments = rstd::move(allocated).unwrap();
        for (int i = 0; i < count; ++i)
            arguments.push(FtsArgument { slice<u8>::from_raw_parts(
                reinterpret_cast<const rstd::byte*>(args[i]), usize(std::strlen(args[i]))) });
        auto created = owner->factory->operator()(arguments.as_slice());
        if (created.is_err()) {
            auto failure = rstd::move(created).unwrap_err();
            int  code    = failure.kind == ErrorKind::Cancelled ? SQLITE_INTERRUPT : SQLITE_ERROR;
            db.callback_error = Some(rstd::move(failure));
            return code;
        }
        auto token = Arc<FtsTokenOwner>::try_make(FtsTokenOwner { rstd::move(created).unwrap() });
        if (token.is_err()) return SQLITE_NOMEM;
        auto  tokenizer = rstd::move(token).unwrap();
        usize index {};
        while (index < owner->instances.len() && owner->instances[index].is_some()) ++index;
        if (index == owner->instances.len()) {
            if (owner->instances.try_reserve(usize(1)).is_err()) return SQLITE_NOMEM;
            owner->instances.push(None());
        }
        auto instance = alloc::boxed::Box<FtsInstance>::try_make(
            FtsInstance { &db, owner.downgrade(), tokenizer.downgrade(), index });
        if (instance.is_err()) return SQLITE_NOMEM;
        owner->instances[index] = Some(rstd::move(tokenizer));
        *out =
            reinterpret_cast<Fts5Tokenizer*>(rstd::move(instance).unwrap().into_raw().as_raw_ptr());
        return SQLITE_OK;
    }
    static void destroy(Fts5Tokenizer* raw) noexcept {
        auto instance = alloc::boxed::Box<FtsInstance>::from_raw(
            rstd::mut_ptr<FtsInstance>::from_raw_parts(reinterpret_cast<FtsInstance*>(raw)));
        auto owner = instance->owner.upgrade();
        if (owner && instance->index < owner->instances.len())
            owner->instances[instance->index] = None();
    }
    static auto tokenize(Fts5Tokenizer* raw,
                         void*          context,
                         int            flags,
                         const char*    text,
                         int            size,
                         const char*    locale,
                         int            locale_size,
                         int (*emit)(void*, int, const char*, int, int, int)) noexcept -> int {
        auto& instance = *reinterpret_cast<FtsInstance*>(raw);
        auto& db       = *instance.database;
        if (db.cancelled()) return SQLITE_INTERRUPT;
        auto owner = instance.owner.upgrade();
        auto token = instance.tokenizer.upgrade();
        if (! owner || ! token) {
            db.callback_error = Some(FtsFailure {
                ErrorKind::State, SQLITE_ABORT, String::make("FTS registration expired"_str) });
            return SQLITE_ABORT;
        }
        if (size < 0 || locale_size < 0 || (! text && size) || (! locale && locale_size) || ! emit)
            return SQLITE_MISUSE;
        FtsMode mode;
        switch (flags) {
        case FTS5_TOKENIZE_DOCUMENT: mode = FtsMode::Document; break;
        case FTS5_TOKENIZE_QUERY: mode = FtsMode::Query; break;
        case FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX: mode = FtsMode::QueryPrefix; break;
        case FTS5_TOKENIZE_AUX: mode = FtsMode::Auxiliary; break;
        default: return SQLITE_MISUSE;
        }
        static const rstd::byte empty_byte {};
        auto                    text_bytes = slice<u8>::from_raw_parts(
            text ? reinterpret_cast<const rstd::byte*>(text) : &empty_byte, usize(size));
        Option<slice<u8>> locale_bytes;
        if (locale)
            locale_bytes = Some(slice<u8>::from_raw_parts(
                reinterpret_cast<const rstd::byte*>(locale), usize(locale_size)));
        const Cancellation* cancellation = db.cancellation.is_some() ? &*db.cancellation : nullptr;
        FtsInput            input(text_bytes, mode, locale_bytes, cancellation);
        FtsTokenSink        sink(db, context, emit, usize(size));
        auto                result = token->tokenizer->operator()(input, sink);
        if (sink.failure_.is_some()) {
            auto failure      = sink.failure_.take().unwrap();
            int  code         = failure.code;
            db.callback_error = Some(rstd::move(failure));
            return code;
        }
        if (result.is_err()) {
            auto failure = rstd::move(result).unwrap_err();
            int  code    = failure.kind == ErrorKind::Cancelled ? SQLITE_INTERRUPT : SQLITE_ERROR;
            db.callback_error = Some(rstd::move(failure));
            return code;
        }
        return db.cancelled() ? SQLITE_INTERRUPT : SQLITE_OK;
    }
};
struct ScalarAdapter {
    static void fail(sqlite3_context* context, const CallbackFailure& failure, int code) noexcept {
        auto bytes = failure.message.as_str().as_bytes();
        if (bytes.len() > usize(INT_MAX)) {
            if (code == SQLITE_INTERRUPT)
                sqlite3_result_error_code(context, code);
            else
                sqlite3_result_error_toobig(context);
            return;
        }
        auto text = bytes.is_empty() ? "" : reinterpret_cast<const char*>(bytes.as_raw_ptr());
        sqlite3_result_error(context, text, int(bytes.len().to_primitive()));
        if (code != SQLITE_ERROR) sqlite3_result_error_code(context, code);
    }
    static void invoke(sqlite3_context* context, int count, sqlite3_value** args) noexcept {
        auto& registration = *static_cast<ScalarContext*>(sqlite3_user_data(context));
        auto& db           = *registration.database;
        if (db.cancelled()) {
            sqlite3_result_error_code(context, SQLITE_INTERRUPT);
            return;
        }
        auto owner = registration.owner.upgrade();
        if (! owner) {
            CallbackFailure failure { ErrorKind::State,
                                      SQLITE_ABORT,
                                      String::make("scalar registration expired"_str) };
            fail(context, failure, SQLITE_ABORT);
            db.callback_error = Some(rstd::move(failure));
            return;
        }
        if (count != 1 || ! args || ! args[0]) {
            sqlite3_result_error_code(context, SQLITE_MISUSE);
            return;
        }
        Option<slice<u8>> value;
        if (sqlite3_value_type(args[0]) != SQLITE_NULL) {
            auto bytes = sqlite3_value_text(args[0]);
            if (! bytes) {
                sqlite3_result_error_nomem(context);
                return;
            }
            int length = sqlite3_value_bytes(args[0]);
            value      = Some(slice<u8>::from_raw_parts(reinterpret_cast<const rstd::byte*>(bytes),
                                                        usize(length)));
        }
        const Cancellation* cancellation = db.cancellation.is_some() ? &*db.cancellation : nullptr;
        ScalarInput         input(value, cancellation);
        auto                result = owner->function->operator()(input);
        if (result.is_err()) {
            auto failure = rstd::move(result).unwrap_err();
            int  code    = failure.kind == ErrorKind::Cancelled ? SQLITE_INTERRUPT : SQLITE_ERROR;
            fail(context, failure, code);
            db.callback_error = Some(rstd::move(failure));
            return;
        }
        if (db.cancelled()) {
            sqlite3_result_error_code(context, SQLITE_INTERRUPT);
            return;
        }
        auto result_value = rstd::move(result).unwrap();
        if (result_value.is_none()) {
            sqlite3_result_null(context);
            return;
        }
        auto text  = rstd::move(result_value).unwrap();
        auto bytes = text.as_str().as_bytes();
        auto ptr   = bytes.is_empty() ? "" : reinterpret_cast<const char*>(bytes.as_raw_ptr());
        sqlite3_result_text64(
            context, ptr, bytes.len().to_primitive(), SQLITE_TRANSIENT, SQLITE_UTF8);
    }
};
} // namespace sqlito

export namespace sqlito
{
class Connection;
class Transaction;
class Statement {
    friend class Connection;
    friend class Transaction;
    Arc<Database>            database_;
    sqlite3_stmt*            handle_ {};
    Database::StatementNode* node_ {};
    unsigned long long       token_ {};
    enum class State
    {
        Ready,
        Row,
        Done,
        Failed
    };
    State     state_ { State::Ready };
    Vec<bool> bound_;
    Statement(Arc<Database>            db,
              sqlite3_stmt*            stmt,
              Database::StatementNode* node,
              unsigned long long       token)
        : database_(rstd::move(db)), handle_(stmt), node_(node), token_(token) {
        ++database_->statements;
        for (int i = 0; i < sqlite3_bind_parameter_count(stmt); ++i) bound_.push(false);
    }
    auto binding(usize index) -> Result<int, Error> {
        if (! handle_ || ! database_->valid(token_) || state_ != State::Ready)
            return Err(error(ErrorKind::State, "statement is not ready to bind"_str));
        if (index == usize() || index > bound_.len())
            return Err(error(ErrorKind::Range, "parameter index out of range"_str));
        return Ok(int(index.to_primitive()));
    }
    auto bound(usize index, int code) -> Result<empty, Error> {
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        bound_[index - usize(1)] = true;
        return Ok(empty {});
    }
    auto column(usize index) -> Result<int, Error> {
        if (! handle_ || ! database_->valid(token_) || state_ != State::Row)
            return Err(error(ErrorKind::State, "no current row"_str));
        if (index >= usize(sqlite3_column_count(handle_)))
            return Err(error(ErrorKind::Range, "column index out of range"_str));
        return Ok(int(index.to_primitive()));
    }

public:
    Statement(const Statement&)                    = delete;
    auto operator=(const Statement&) -> Statement& = delete;
    Statement(Statement&& other) noexcept
        : database_(rstd::move(other.database_)),
          handle_(rstd::exchange(other.handle_, nullptr)),
          node_(rstd::exchange(other.node_, nullptr)),
          token_(other.token_),
          state_(other.state_),
          bound_(rstd::move(other.bound_)) {}
    auto operator=(Statement&& other) noexcept -> Statement& {
        if (this != &other) {
            if (node_) database_->release_statement(node_);
            database_ = rstd::move(other.database_);
            handle_   = rstd::exchange(other.handle_, nullptr);
            node_     = rstd::exchange(other.node_, nullptr);
            token_    = other.token_;
            state_    = other.state_;
            bound_    = rstd::move(other.bound_);
        }
        return *this;
    }
    ~Statement() {
        if (node_) database_->release_statement(node_);
    }
    auto bind(usize index, i64 value) -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(binding(index));
        return complete(call,
                        bound(index, sqlite3_bind_int64(handle_, slot, value.to_primitive())));
    }
    auto bind_text(usize index, ref<str> value) -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(binding(index));
        auto ptr =
            value.is_empty() ? "" : reinterpret_cast<const char*>(value.as_bytes().as_raw_ptr());
        return complete(call,
                        bound(index,
                              sqlite3_bind_text64(handle_,
                                                  slot,
                                                  ptr,
                                                  value.len().to_primitive(),
                                                  SQLITE_TRANSIENT,
                                                  SQLITE_UTF8)));
    }
    auto bind_blob(usize index, slice<u8> value) -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto        slot = rstd_try(binding(index));
        const void* ptr  = value.is_empty() ? static_cast<const void*>("")
                                            : static_cast<const void*>(value.as_raw_ptr());
        return complete(
            call,
            bound(index,
                  sqlite3_bind_blob64(
                      handle_, slot, ptr, value.len().to_primitive(), SQLITE_TRANSIENT)));
    }
    auto bind_null(usize index) -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(binding(index));
        return complete(call, bound(index, sqlite3_bind_null(handle_, slot)));
    }
    auto step() -> Result<Step, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! handle_ || ! database_->valid(token_) ||
            (state_ != State::Ready && state_ != State::Row))
            return Err(error(ErrorKind::State, "statement requires reset"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        for (bool present : bound_)
            if (! present) return Err(error(ErrorKind::Unbound, "parameter is not bound"_str));
        database_->callback_error = None();
        int code                  = sqlite3_step(handle_);
        if (code != SQLITE_ROW && code != SQLITE_DONE) {
            state_ = State::Failed;
            return Err(database_error(*database_, code));
        }
        if (call.finish()) {
            state_ = State::Failed;
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        }
        if (code == SQLITE_ROW) {
            state_ = State::Row;
            return Ok(Step::Row);
        }
        if (code == SQLITE_DONE) {
            state_ = State::Done;
            return Ok(Step::Done);
        }
        return Err(error(ErrorKind::State, "unexpected SQLite step result"_str));
    }
    auto reset() -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! handle_ || ! database_->valid(token_))
            return Err(error(ErrorKind::State, "moved statement"_str));
        database_->callback_error = None();
        int code                  = sqlite3_reset(handle_);
        (void)sqlite3_clear_bindings(handle_);
        for (auto& present : bound_) present = false;
        state_ = State::Ready;
        if (code != SQLITE_OK) return Err(database_error(*database_, code));
        database_->callback_error = None();
        return complete(call, Result<empty, Error>(Ok(empty {})));
    }
    auto column_type(usize index) -> Result<ColumnType, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto       slot = rstd_try(column(index));
        ColumnType type;
        switch (sqlite3_column_type(handle_, slot)) {
        case SQLITE_NULL: type = ColumnType::Null; break;
        case SQLITE_INTEGER: type = ColumnType::Integer; break;
        case SQLITE_FLOAT: type = ColumnType::Real; break;
        case SQLITE_TEXT: type = ColumnType::Text; break;
        case SQLITE_BLOB: type = ColumnType::Blob; break;
        default: return Err(error(ErrorKind::Type, "unknown column type"_str));
        }
        return complete(call, Result<ColumnType, Error>(Ok(type)));
    }
    auto integer(usize index) -> Result<Option<i64>, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL)
            return complete(call, Result<Option<i64>, Error>(Ok<Option<i64>>(None())));
        if (type != SQLITE_INTEGER)
            return Err(error(ErrorKind::Type, "column is not an integer"_str));
        return complete(
            call, Result<Option<i64>, Error>(Ok(Some(i64(sqlite3_column_int64(handle_, slot))))));
    }
    auto real(usize index) -> Result<Option<double>, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL)
            return complete(call, Result<Option<double>, Error>(Ok<Option<double>>(None())));
        if (type != SQLITE_FLOAT) return Err(error(ErrorKind::Type, "column is not real"_str));
        return complete(
            call, Result<Option<double>, Error>(Ok(Some(sqlite3_column_double(handle_, slot)))));
    }
    auto blob(usize index) -> Result<Option<Vec<u8>>, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL)
            return complete(call, Result<Option<Vec<u8>>, Error>(Ok<Option<Vec<u8>>>(None())));
        if (type != SQLITE_BLOB) return Err(error(ErrorKind::Type, "column is not a blob"_str));
        auto ptr = sqlite3_column_blob(handle_, slot);
        if (! ptr) {
            auto code = sqlite3_errcode(database_->handle);
            if (code == SQLITE_NOMEM) return Err(database_error(database_->handle, code));
            return complete(call, Result<Option<Vec<u8>>, Error>(Ok(Some(Vec<u8> {}))));
        }
        auto length = usize(sqlite3_column_bytes(handle_, slot));
        auto bytes  = slice<u8>::from_raw_parts(static_cast<const rstd::byte*>(ptr), length);
        auto value  = Vec<u8>::with_capacity(length);
        for (auto byte : bytes) value.push(rstd::move(byte));
        return complete(call, Result<Option<Vec<u8>>, Error>(Ok(Some(rstd::move(value)))));
    }
    auto text(usize index) -> Result<Option<String>, Error> {
        if (! database_) return Err(error(ErrorKind::State, "statement is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL)
            return complete(call, Result<Option<String>, Error>(Ok<Option<String>>(None())));
        if (type != SQLITE_TEXT) return Err(error(ErrorKind::Type, "column is not text"_str));
        auto ptr = sqlite3_column_text(handle_, slot);
        if (! ptr) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto bytes = slice<u8>::from_raw_parts(reinterpret_cast<const rstd::byte*>(ptr),
                                               usize(sqlite3_column_bytes(handle_, slot)));
        auto value =
            rstd_try(rstd::str_::from_utf8(bytes), error(ErrorKind::Type, "text is not UTF-8"_str));
        return complete(call, Result<Option<String>, Error>(Ok(Some(String::make(value)))));
    }
};

} // namespace sqlito

namespace sqlito
{
auto execute_batch_owned(const Arc<Database>& db, ref<str> sql, unsigned long long token)
    -> Result<empty, Error> {
    if (! db) return Err(error(ErrorKind::State, "session is unavailable"_str));
    DatabaseCall call(db);
    if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
    if (! db || ! db->valid(token))
        return Err(error(ErrorKind::State, "session is unavailable"_str));
    auto bytes  = rstd_try(terminated(sql));
    auto cursor = reinterpret_cast<const char*>(bytes.data());
    while (*cursor) {
        if (! db->valid(token)) return Err(error(ErrorKind::State, "session is unavailable"_str));
        if (db->cancelled()) return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        sqlite3_stmt* statement = nullptr;
        const char*   tail      = nullptr;
        db->callback_error      = None();
        auto code               = sqlite3_prepare_v2(db->handle, cursor, -1, &statement, &tail);
        struct Finalize {
            sqlite3_stmt* statement;
            ~Finalize() {
                if (statement) (void)sqlite3_finalize(statement);
            }
        } cleanup { statement };
        if (code != SQLITE_OK) return Err(database_error(*db, code));
        if (call.rollback_pending())
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        if (tail == cursor) return Err(error(ErrorKind::Input, "SQL parser made no progress"_str));
        cursor = tail;
        if (! statement) continue;
        if (sqlite3_bind_parameter_count(statement) != 0)
            return Err(error(ErrorKind::Unbound, "batch statements cannot contain parameters"_str));
        do {
            db->callback_error = None();
            code               = sqlite3_step(statement);
        } while (code == SQLITE_ROW);
        if (code != SQLITE_DONE) return Err(database_error(*db, code));
        if (call.rollback_pending())
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
    }
    if (call.finish())
        return Err(error(ErrorKind::State, "transaction released during operation"_str));
    return Ok(empty {});
}

} // namespace sqlito

export namespace sqlito
{
// Synchronous session: serialize all operations on the connection and its
// statements.
class Connection {
    friend class Transaction;
    Arc<Database> database_;
    explicit Connection(Arc<Database> db): database_(rstd::move(db)) {}

public:
    Connection(const Connection&)                               = delete;
    auto operator=(const Connection&) -> Connection&            = delete;
    Connection(Connection&&) noexcept                           = default;
    auto        operator=(Connection&&) noexcept -> Connection& = default;
    static auto open(ref<str> path) -> Result<Connection, Error> {
        auto     name   = rstd_try(terminated(path));
        sqlite3* handle = nullptr;
        int      code =
            sqlite3_open_v2(reinterpret_cast<const char*>(name.data()),
                            &handle,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                            nullptr);
        if (code != SQLITE_OK) {
            auto failure = database_error(handle, code);
            if (handle) (void)sqlite3_close(handle);
            return Err(rstd::move(failure));
        }
        (void)sqlite3_extended_result_codes(handle, 1);
        auto database = Arc<Database>::make(handle);
        code          = sqlite3_set_authorizer(
            handle,
            [](void* context, int action, const char*, const char*, const char*, const char*) {
                auto& db = *static_cast<Database*>(context);
                return (action == SQLITE_TRANSACTION || action == SQLITE_SAVEPOINT) &&
                               ! db.control_allowed
                           ? SQLITE_DENY
                           : SQLITE_OK;
            },
            &*database);
        if (code != SQLITE_OK) return Err(database_error(handle, code));
        return Ok(Connection { rstd::move(database) });
    }
    auto close() -> Result<empty, Error> {
        if (! database_) return Ok(empty {});
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! database_->handle) return Ok(empty {});
        if (! database_->valid(0) || database_->cancellation.is_some())
            return Err(error(ErrorKind::State, "connection is occupied"_str));
        database_->callback_error = None();
        int code                  = sqlite3_close(database_->handle);
        if (code != SQLITE_OK) return Err(database_error(*database_, code));
        database_->handle         = nullptr;
        database_->callback_error = None();
        return Ok(empty {});
    }
    auto prepare(ref<str> sql) -> Result<Statement, Error> {
        return prepare_owned(database_, sql, 0);
    }
    // Replaces the name on this connection. Keep the returned owner alive until
    // dependent FTS operations have ended; SQLite holds only weak references.
    auto register_or_replace_fts5_tokenizer(String name, FtsFactory factory)
        -> Result<FtsRegistration, Error> {
        if (! database_) return Err(error(ErrorKind::State, "connection is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! database_->valid(0))
            return Err(error(ErrorKind::State, "connection is unavailable"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        if (name.is_empty()) return Err(error(ErrorKind::Input, "empty tokenizer name"_str));
        auto          bytes       = rstd_try(terminated(name.as_str()));
        sqlite3_stmt* stmt        = nullptr;
        fts5_api*     api         = nullptr;
        database_->callback_error = None();
        int code = sqlite3_prepare_v2(database_->handle, "SELECT fts5(?1)", -1, &stmt, nullptr);
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        code = sqlite3_bind_pointer(stmt, 1, &api, "fts5_api_ptr", nullptr);
        if (code == SQLITE_OK) {
            code = sqlite3_step(stmt);
            if (code == SQLITE_ROW) code = SQLITE_OK;
        }
        int finalized = sqlite3_finalize(stmt);
        if (code == SQLITE_OK && finalized != SQLITE_OK) code = finalized;
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        if (! api || api->iVersion < 3 || ! api->xCreateTokenizer_v2)
            return Err(error(ErrorKind::State, "FTS5 tokenizer v2 unavailable"_str));
        auto owner_result = Arc<FtsOwner>::try_make(rstd::move(factory));
        if (owner_result.is_err()) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto owner = rstd::move(owner_result).unwrap();
        auto context_result =
            alloc::boxed::Box<FtsContext>::try_make(FtsContext { &*database_, owner.downgrade() });
        if (context_result.is_err()) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto              context = rstd::move(context_result).unwrap();
        fts5_tokenizer_v2 methods {
            2, FtsAdapter::create, FtsAdapter::destroy, FtsAdapter::tokenize
        };
        code = api->xCreateTokenizer_v2(
            api, reinterpret_cast<const char*>(bytes.data()), &*context, &methods, [](void* raw) {
                auto owned = alloc::boxed::Box<FtsContext>::from_raw(
                    rstd::mut_ptr<FtsContext>::from_raw_parts(static_cast<FtsContext*>(raw)));
            });
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        (void)rstd::move(context).into_raw();
        if (call.finish())
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        return Ok(FtsRegistration(rstd::move(owner)));
    }
    // Replaces the one-argument function on this connection. SQLite retains a
    // weak reference; keep the owner alive for all dependent statements.
    auto register_or_replace_scalar_function(String         name,
                                             ScalarFunction function,
                                             bool           deterministic = false)
        -> Result<ScalarRegistration, Error> {
        if (! database_) return Err(error(ErrorKind::State, "connection is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! database_->valid(0))
            return Err(error(ErrorKind::State, "connection is unavailable"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        if (name.is_empty()) return Err(error(ErrorKind::Input, "empty scalar function name"_str));
        auto bytes        = rstd_try(terminated(name.as_str()));
        auto owner_result = Arc<ScalarOwner>::try_make(rstd::move(function));
        if (owner_result.is_err()) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto owner          = rstd::move(owner_result).unwrap();
        auto context_result = alloc::boxed::Box<ScalarContext>::try_make(
            ScalarContext { &*database_, owner.downgrade() });
        if (context_result.is_err()) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto raw                  = rstd::move(context_result).unwrap().into_raw().as_raw_ptr();
        database_->callback_error = None();
        int flags                 = SQLITE_UTF8 | (deterministic ? SQLITE_DETERMINISTIC : 0);
        int code =
            sqlite3_create_function_v2(database_->handle,
                                       reinterpret_cast<const char*>(bytes.data()),
                                       1,
                                       flags,
                                       raw,
                                       ScalarAdapter::invoke,
                                       nullptr,
                                       nullptr,
                                       [](void* value) {
                                           auto owned = alloc::boxed::Box<ScalarContext>::from_raw(
                                               rstd::mut_ptr<ScalarContext>::from_raw_parts(
                                                   static_cast<ScalarContext*>(value)));
                                       });
        if (code != SQLITE_OK) return Err(database_error(*database_, code));
        if (call.finish())
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        return Ok(ScalarRegistration(rstd::move(owner)));
    }
    // Connection-local SQLite changes counter, not a result owned by a Statement.
    // Read immediately after the target INSERT, UPDATE or DELETE reaches Done.
    // A later DML can overwrite it; SELECT, DDL, errors and resets do not
    // establish a count for that operation.
    // Trigger, foreign-key and REPLACE auxiliary changes are excluded. A count
    // inside a transaction does not imply that the transaction will commit.
    auto last_changes() -> Result<u64, Error> {
        if (! database_) return Err(error(ErrorKind::State, "connection is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! database_ || ! database_->valid(0))
            return Err(error(ErrorKind::State, "connection is unavailable"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        return complete(call,
                        Result<u64, Error>(Ok(u64(static_cast<unsigned long long>(
                            sqlite3_changes64(database_->handle))))));
    }
    // Batches are not implicitly atomic; use a Transaction when all statements must commit together.
    auto execute_batch(ref<str> sql) -> Result<empty, Error> {
        return execute_batch_owned(database_, sql, 0);
    }
    auto idle() -> bool {
        if (! database_) return false;
        DatabaseCall call(database_);
        if (! call.entered) return false;
        return database_ && database_->valid(0) && database_->cancellation.is_none() &&
               database_->statements == 0;
    }
    auto begin(TransactionMode mode = TransactionMode::Deferred) -> Result<Transaction, Error>;

    // Synchronous callback; keep all statements and transactions inside its
    // scope. Cancellation is cooperative and does not undo a completed commit.
    template<class F>
    auto run(const Cancellation& cancel, F&& function) -> decltype(function(*this)) {
        if (! database_) return Err(error(ErrorKind::State, "connection is unavailable"_str));
        {
            DatabaseCall call(database_);
            if (! call.entered || ! database_->valid(0) || database_->cancellation.is_some())
                return Err(error(ErrorKind::State, "connection is unavailable"_str));
            if (cancel.cancelled())
                return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
            database_->cancellation = Some(cancel.clone());
            sqlite3_progress_handler(
                database_->handle,
                1000,
                [](void* context) {
                    auto& db = *static_cast<Database*>(context);
                    return ! db.control_allowed && db.cancelled() ? 1 : 0;
                },
                &*database_);
        }
        struct Scope {
            Arc<Database> database;
            ~Scope() {
                database->enter_wait();
                sqlite3_progress_handler(database->handle, 0, nullptr, nullptr);
                database->cancellation = None();
                database->drain();
            }
        } scope { database_.clone() };
        return function(*this);
    }

private:
    static auto prepare_owned(const Arc<Database>& database_,
                              ref<str>             sql,
                              unsigned long long   token) -> Result<Statement, Error> {
        if (! database_) return Err(error(ErrorKind::State, "closed connection"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! database_->valid(token)) return Err(error(ErrorKind::State, "closed connection"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        if (sql.len() >= usize(2147483647))
            return Err(error(ErrorKind::Input, "SQL exceeds byte limit"_str));
        auto          bytes       = rstd_try(terminated(sql));
        sqlite3_stmt* statement   = nullptr;
        const char*   tail        = nullptr;
        database_->callback_error = None();
        int code                  = sqlite3_prepare_v3(database_->handle,
                                                       reinterpret_cast<const char*>(bytes.data()),
                                                       int(bytes.len().to_primitive()),
                                                       0,
                                                       &statement,
                                                       &tail);
        if (code != SQLITE_OK) return Err(database_error(*database_, code));
        if (call.rollback_pending()) {
            (void)sqlite3_finalize(statement);
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        }
        if (! statement) return Err(error(ErrorKind::Input, "empty statement"_str));
        for (; *tail; ++tail) {
            if (*tail != ' ' && *tail != '\t' && *tail != '\r' && *tail != '\n' && *tail != '\f') {
                (void)sqlite3_finalize(statement);
                return Err(error(ErrorKind::Input, "trailing SQL is not allowed"_str));
            }
        }
        auto node = alloc::boxed::Box<Database::StatementNode>::try_make(
            Database::StatementNode { statement, nullptr });
        if (node.is_err()) {
            (void)sqlite3_finalize(statement);
            return Err(database_error(database_->handle, SQLITE_NOMEM));
        }
        auto ptr   = rstd::move(node).unwrap().into_raw().as_raw_ptr();
        auto value = Statement { database_.clone(), statement, ptr, token };
        if (call.finish()) {
            return Err(error(ErrorKind::State, "transaction released during operation"_str));
        }
        return Ok(rstd::move(value));
    }
};

class Transaction {
    friend class Connection;
    Arc<Database>      database_;
    unsigned long long token_ {};
    Transaction(Arc<Database> db, unsigned long long token)
        : database_(rstd::move(db)), token_(token) {}
    auto finish(const char* sql) -> Result<empty, Error> {
        if (! token_ || ! database_ || ! database_->valid(token_))
            return Err(error(ErrorKind::State, "transaction is not active"_str));
        auto result = database_->control(sql);
        if (sqlite3_get_autocommit(database_->handle)) {
            database_->active = 0;
            token_            = 0;
        }
        return result;
    }
    void release() {
        if (token_ && database_) {
            database_->defer_rollback(token_);
            token_ = 0;
            return;
        }
    }

public:
    Transaction(const Transaction&)                    = delete;
    auto operator=(const Transaction&) -> Transaction& = delete;
    Transaction(Transaction&& other) noexcept
        : database_(rstd::move(other.database_)), token_(rstd::exchange(other.token_, 0)) {}
    auto operator=(Transaction&& other) -> Transaction& {
        if (this != &other) {
            release();
            database_ = rstd::move(other.database_);
            token_    = rstd::exchange(other.token_, 0);
        }
        return *this;
    }
    ~Transaction() { release(); }
    auto prepare(ref<str> sql) -> Result<Statement, Error> {
        if (! token_) return Err(error(ErrorKind::State, "transaction is not active"_str));
        return Connection::prepare_owned(database_, sql, token_);
    }
    // Same connection-local counter and timing requirements as Connection::last_changes().
    auto last_changes() -> Result<u64, Error> {
        if (! database_) return Err(error(ErrorKind::State, "transaction is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (! token_ || ! database_ || ! database_->valid(token_))
            return Err(error(ErrorKind::State, "transaction is not active"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        return complete(call,
                        Result<u64, Error>(Ok(u64(static_cast<unsigned long long>(
                            sqlite3_changes64(database_->handle))))));
    }
    auto commit() -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "transaction is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        if (database_ && database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        return finish("COMMIT");
    }
    auto rollback() -> Result<empty, Error> {
        if (! database_) return Err(error(ErrorKind::State, "transaction is unavailable"_str));
        DatabaseCall call(database_);
        if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
        return finish("ROLLBACK");
    }
    auto execute_batch(ref<str> sql) -> Result<empty, Error> {
        if (! token_) return Err(error(ErrorKind::State, "transaction is not active"_str));
        return execute_batch_owned(database_, sql, token_);
    }
};

auto Connection::begin(TransactionMode mode) -> Result<Transaction, Error> {
    if (! database_) return Err(error(ErrorKind::State, "connection is unavailable"_str));
    DatabaseCall call(database_);
    if (! call.entered) return Err(error(ErrorKind::State, "connection is occupied"_str));
    if (! database_ || ! database_->valid(0) || database_->generation == ~0ULL)
        return Err(error(ErrorKind::State, "connection is unavailable"_str));
    if (database_->cancelled()) return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
    for (auto* stmt = sqlite3_next_stmt(database_->handle, nullptr); stmt;
         stmt       = sqlite3_next_stmt(database_->handle, stmt))
        if (sqlite3_stmt_busy(stmt))
            return Err(error(ErrorKind::State, "statement is still active"_str));
    const char* sql = "BEGIN DEFERRED";
    switch (mode) {
    case TransactionMode::Deferred: break;
    case TransactionMode::Immediate: sql = "BEGIN IMMEDIATE"; break;
    case TransactionMode::Exclusive: sql = "BEGIN EXCLUSIVE"; break;
    default: return Err(error(ErrorKind::Input, "invalid transaction mode"_str));
    }
    rstd_try(database_->control(sql));
    database_->active = ++database_->generation;
    return Ok(Transaction { database_.clone(), database_->active });
}
} // namespace sqlito

module;
#include <sqlite3.h>
#include <rstd/macro.hpp>
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
struct Error {
    ErrorKind kind;
    int       code;
    String    message;
};
enum class Step
{
    Row,
    Done
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
struct Database {
    sqlite3*             handle;
    unsigned long long   active {}, generation {};
    unsigned long long   statements {};
    bool                 control_allowed {}, poisoned {};
    Option<Cancellation> cancellation;
    explicit Database(sqlite3* value): handle(value) {}
    auto cancelled() const -> bool { return cancellation.is_some() && cancellation->cancelled(); }
    auto valid(unsigned long long token) -> bool {
        if (! handle || poisoned) return false;
        if (active && sqlite3_get_autocommit(handle)) active = 0;
        return active == token;
    }
    auto control(const char* sql) -> Result<empty, Error> {
        control_allowed = true;
        int code        = sqlite3_exec(handle, sql, nullptr, nullptr, nullptr);
        control_allowed = false;
        if (code != SQLITE_OK) return Err(database_error(handle, code));
        return Ok(empty {});
    }
    ~Database() {
        if (handle) (void)sqlite3_close_v2(handle);
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
    Arc<Database>      database_;
    sqlite3_stmt*      handle_ {};
    unsigned long long token_ {};
    enum class State
    {
        Ready,
        Row,
        Done,
        Failed
    };
    State     state_ { State::Ready };
    Vec<bool> bound_;
    Statement(Arc<Database> db, sqlite3_stmt* stmt, unsigned long long token)
        : database_(rstd::move(db)), handle_(stmt), token_(token) {
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
          token_(other.token_),
          state_(other.state_),
          bound_(rstd::move(other.bound_)) {}
    auto operator=(Statement&& other) noexcept -> Statement& {
        if (this != &other) {
            if (handle_) {
                (void)sqlite3_finalize(handle_);
                --database_->statements;
            }
            database_ = rstd::move(other.database_);
            handle_   = rstd::exchange(other.handle_, nullptr);
            token_    = other.token_;
            state_    = other.state_;
            bound_    = rstd::move(other.bound_);
        }
        return *this;
    }
    ~Statement() {
        if (handle_) {
            (void)sqlite3_finalize(handle_);
            --database_->statements;
        }
    }
    auto bind(usize index, i64 value) -> Result<empty, Error> {
        auto slot = rstd_try(binding(index));
        return bound(index, sqlite3_bind_int64(handle_, slot, value.to_primitive()));
    }
    auto bind_text(usize index, ref<str> value) -> Result<empty, Error> {
        auto slot = rstd_try(binding(index));
        auto ptr =
            value.is_empty() ? "" : reinterpret_cast<const char*>(value.as_bytes().as_raw_ptr());
        return bound(
            index,
            sqlite3_bind_text64(
                handle_, slot, ptr, value.len().to_primitive(), SQLITE_TRANSIENT, SQLITE_UTF8));
    }
    auto bind_blob(usize index, slice<u8> value) -> Result<empty, Error> {
        auto        slot = rstd_try(binding(index));
        const void* ptr  = value.is_empty() ? static_cast<const void*>("")
                                            : static_cast<const void*>(value.as_raw_ptr());
        return bound(
            index,
            sqlite3_bind_blob64(handle_, slot, ptr, value.len().to_primitive(), SQLITE_TRANSIENT));
    }
    auto bind_null(usize index) -> Result<empty, Error> {
        auto slot = rstd_try(binding(index));
        return bound(index, sqlite3_bind_null(handle_, slot));
    }
    auto step() -> Result<Step, Error> {
        if (! handle_ || ! database_->valid(token_) ||
            (state_ != State::Ready && state_ != State::Row))
            return Err(error(ErrorKind::State, "statement requires reset"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        for (bool present : bound_)
            if (! present) return Err(error(ErrorKind::Unbound, "parameter is not bound"_str));
        int code = sqlite3_step(handle_);
        if (code == SQLITE_ROW) {
            state_ = State::Row;
            return Ok(Step::Row);
        }
        if (code == SQLITE_DONE) {
            state_ = State::Done;
            return Ok(Step::Done);
        }
        state_ = State::Failed;
        return Err(database_error(database_->handle, code));
    }
    auto reset() -> Result<empty, Error> {
        if (! handle_ || ! database_->valid(token_))
            return Err(error(ErrorKind::State, "moved statement"_str));
        int code = sqlite3_reset(handle_);
        (void)sqlite3_clear_bindings(handle_);
        for (auto& present : bound_) present = false;
        state_ = State::Ready;
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        return Ok(empty {});
    }
    auto integer(usize index) -> Result<Option<i64>, Error> {
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL) return Ok<Option<i64>>(None());
        if (type != SQLITE_INTEGER)
            return Err(error(ErrorKind::Type, "column is not an integer"_str));
        return Ok(Some(i64(sqlite3_column_int64(handle_, slot))));
    }
    auto blob(usize index) -> Result<Option<Vec<u8>>, Error> {
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL) return Ok<Option<Vec<u8>>>(None());
        if (type != SQLITE_BLOB) return Err(error(ErrorKind::Type, "column is not a blob"_str));
        auto ptr = sqlite3_column_blob(handle_, slot);
        if (! ptr) {
            auto code = sqlite3_errcode(database_->handle);
            if (code == SQLITE_NOMEM) return Err(database_error(database_->handle, code));
            return Ok(Some(Vec<u8> {}));
        }
        auto length = usize(sqlite3_column_bytes(handle_, slot));
        auto bytes  = slice<u8>::from_raw_parts(static_cast<const rstd::byte*>(ptr), length);
        auto value  = Vec<u8>::with_capacity(length);
        for (auto byte : bytes) value.push(rstd::move(byte));
        return Ok(Some(rstd::move(value)));
    }
    auto text(usize index) -> Result<Option<String>, Error> {
        auto slot = rstd_try(column(index));
        int  type = sqlite3_column_type(handle_, slot);
        if (type == SQLITE_NULL) return Ok<Option<String>>(None());
        if (type != SQLITE_TEXT) return Err(error(ErrorKind::Type, "column is not text"_str));
        auto ptr = sqlite3_column_text(handle_, slot);
        if (! ptr) return Err(database_error(database_->handle, SQLITE_NOMEM));
        auto bytes = slice<u8>::from_raw_parts(reinterpret_cast<const rstd::byte*>(ptr),
                                               usize(sqlite3_column_bytes(handle_, slot)));
        auto value =
            rstd_try(rstd::str_::from_utf8(bytes), error(ErrorKind::Type, "text is not UTF-8"_str));
        return Ok(Some(String::make(value)));
    }
};

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
        if (! database_ || ! database_->handle) return Ok(empty {});
        if (! database_->valid(0) || database_->cancellation.is_some())
            return Err(error(ErrorKind::State, "connection is occupied"_str));
        int code = sqlite3_close(database_->handle);
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        database_->handle = nullptr;
        return Ok(empty {});
    }
    auto prepare(ref<str> sql) -> Result<Statement, Error> {
        return prepare_owned(database_, sql, 0);
    }
    auto idle() -> bool {
        return database_ && database_->valid(0) && database_->cancellation.is_none() &&
               database_->statements == 0;
    }
    auto begin(TransactionMode mode = TransactionMode::Deferred) -> Result<Transaction, Error>;

    // Synchronous callback; keep all statements and transactions inside its
    // scope. Cancellation is cooperative and does not undo a completed commit.
    template<class F>
    auto run(const Cancellation& cancel, F&& function) -> decltype(function(*this)) {
        if (! database_ || ! database_->valid(0) || database_->cancellation.is_some())
            return Err(error(ErrorKind::State, "connection is unavailable"_str));
        if (cancel.cancelled()) return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        struct Scope {
            Arc<Database> database;
            ~Scope() {
                sqlite3_progress_handler(database->handle, 0, nullptr, nullptr);
                database->cancellation = None();
            }
        } scope { database_.clone() };
        database_->cancellation = Some(cancel.clone());
        sqlite3_progress_handler(
            database_->handle,
            1000,
            [](void* context) {
                auto& db = *static_cast<Database*>(context);
                return ! db.control_allowed && db.cancelled() ? 1 : 0;
            },
            &*database_);
        return function(*this);
    }

private:
    static auto prepare_owned(const Arc<Database>& database_,
                              ref<str>             sql,
                              unsigned long long   token) -> Result<Statement, Error> {
        if (! database_ || ! database_->valid(token))
            return Err(error(ErrorKind::State, "closed connection"_str));
        if (database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        if (sql.len() >= usize(2147483647))
            return Err(error(ErrorKind::Input, "SQL exceeds byte limit"_str));
        auto          bytes     = rstd_try(terminated(sql));
        sqlite3_stmt* statement = nullptr;
        const char*   tail      = nullptr;
        int           code      = sqlite3_prepare_v3(database_->handle,
                                                     reinterpret_cast<const char*>(bytes.data()),
                                                     int(bytes.len().to_primitive()),
                                                     0,
                                                     &statement,
                                                     &tail);
        if (code != SQLITE_OK) return Err(database_error(database_->handle, code));
        if (! statement) return Err(error(ErrorKind::Input, "empty statement"_str));
        for (; *tail; ++tail) {
            if (*tail != ' ' && *tail != '\t' && *tail != '\r' && *tail != '\n' && *tail != '\f') {
                (void)sqlite3_finalize(statement);
                return Err(error(ErrorKind::Input, "trailing SQL is not allowed"_str));
            }
        }
        return Ok(Statement { database_.clone(), statement, token });
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
        if (token_ && database_ && database_->valid(token_)) {
            auto result = finish("ROLLBACK");
            if (result.is_err()) database_->poisoned = true;
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
    auto commit() -> Result<empty, Error> {
        if (database_ && database_->cancelled())
            return Err(error(ErrorKind::Cancelled, "operation cancelled"_str));
        return finish("COMMIT");
    }
    auto rollback() -> Result<empty, Error> { return finish("ROLLBACK"); }
};

auto Connection::begin(TransactionMode mode) -> Result<Transaction, Error> {
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

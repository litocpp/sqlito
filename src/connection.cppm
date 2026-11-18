module;
#include <sqlite3.h>
export module sqlito:connection;
export import :error;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::sync::Arc;
using AtomicBool = rstd::sync::atomic::Atomic<bool>;
using Ordering   = rstd::sync::atomic::Ordering;

namespace sqlito
{
struct Database;
struct BlobNode;
struct StatementNode;
} // namespace sqlito

export namespace sqlito
{
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
enum class OpenMode
{
    ReadOnly,
    ReadWrite,
    ReadWriteCreate
};
enum class TransactionMode
{
    Deferred,
    Immediate,
    Exclusive
};
enum class BlobMode
{
    ReadOnly,
    ReadWrite
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
export namespace sqlito
{
class Connection;
class Transaction;
// Fixed-size incremental I/O. The handle retains its connection owner; all
// operations share the session gate, and transaction handles expire on rollback.
class Blob {
    friend class Connection;
    friend class Transaction;
    Arc<Database>      database_;
    BlobNode*          node_ {};
    unsigned long long token_ {};
    BlobMode           mode_;
    usize              length_;

    Blob(Arc<Database> db, BlobNode* node, unsigned long long token, BlobMode mode);
    auto        ready() -> Result<empty, Error>;
    auto        range(usize count, usize offset) const -> Result<empty, Error>;
    static auto open_owned(const Arc<Database>& database,
                           ref<str>             schema,
                           ref<str>             table,
                           ref<str>             column,
                           i64                  row_id,
                           BlobMode             mode,
                           unsigned long long   token) -> Result<Blob, Error>;

public:
    Blob(const Blob&)                    = delete;
    auto operator=(const Blob&) -> Blob& = delete;
    Blob(Blob&& other) noexcept;
    auto operator=(Blob&& other) noexcept -> Blob&;
    ~Blob();
    // Length is fixed at open. Row updates can expire I/O with SQLite's native error.
    auto len() -> Result<usize, Error>;
    auto read_at(mut_ref<u8[]> buffer, usize offset) -> Result<empty, Error>;
    auto write_at(slice<u8> buffer, usize offset) -> Result<empty, Error>;
    // Explicit close reports SQLite errors; destruction closes best-effort.
    auto close() -> Result<empty, Error>;
};

class Statement {
    friend class Connection;
    friend class Transaction;
    Arc<Database>      database_;
    sqlite3_stmt*      handle_ {};
    StatementNode*     node_ {};
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
    Statement(Arc<Database> db, sqlite3_stmt* stmt, StatementNode* node, unsigned long long token);
    auto binding(usize index) -> Result<int, Error>;
    auto bound(usize index, int code) -> Result<empty, Error>;
    auto column(usize index) -> Result<int, Error>;

public:
    Statement(const Statement&)                    = delete;
    auto operator=(const Statement&) -> Statement& = delete;
    Statement(Statement&& other) noexcept;
    auto operator=(Statement&& other) noexcept -> Statement&;
    ~Statement();
    auto bind(usize index, i64 value) -> Result<empty, Error>;
    auto bind(usize index, double value) -> Result<empty, Error>;
    auto bind_text(usize index, ref<str> value) -> Result<empty, Error>;
    auto bind_blob(usize index, slice<u8> value) -> Result<empty, Error>;
    auto bind_null(usize index) -> Result<empty, Error>;
    auto step() -> Result<Step, Error>;
    auto reset() -> Result<empty, Error>;
    auto column_type(usize index) -> Result<ColumnType, Error>;
    auto integer(usize index) -> Result<Option<i64>, Error>;
    auto real(usize index) -> Result<Option<double>, Error>;
    auto blob(usize index) -> Result<Option<Vec<u8>>, Error>;
    auto text(usize index) -> Result<Option<String>, Error>;
};

} // namespace sqlito

export namespace sqlito
{
// Synchronous session: serialize all operations on the connection and its
// statements and blobs.
class Connection {
    friend class Transaction;
    Arc<Database> database_;
    explicit Connection(Arc<Database> db);
    struct RunScope {
        Arc<Database> database;

        explicit RunScope(Arc<Database> owner);
        RunScope(RunScope&&) noexcept;
        ~RunScope();
    };
    auto enter_run(const Cancellation& cancel) -> Result<RunScope, Error>;

public:
    Connection(const Connection&)                    = delete;
    auto operator=(const Connection&) -> Connection& = delete;
    Connection(Connection&&) noexcept;
    auto operator=(Connection&&) noexcept -> Connection&;
    ~Connection();
    static auto open(ref<str> path, OpenMode mode = OpenMode::ReadWriteCreate)
        -> Result<Connection, Error>;
    auto close() -> Result<empty, Error>;
    auto prepare(ref<str> sql) -> Result<Statement, Error>;
    // Close all blobs before begin/close. A Blob can outlive this Connection object.
    auto open_blob(ref<str> schema,
                   ref<str> table,
                   ref<str> column,
                   i64      row_id,
                   BlobMode mode = BlobMode::ReadOnly) -> Result<Blob, Error>;
    // Replaces the name on this connection. Keep the returned owner alive until
    // dependent FTS operations have ended; SQLite holds only weak references.
    auto register_or_replace_fts5_tokenizer(String name, FtsFactory factory)
        -> Result<FtsRegistration, Error>;
    // Replaces the one-argument function on this connection. SQLite retains a
    // weak reference; keep the owner alive for all dependent statements.
    auto register_or_replace_scalar_function(String         name,
                                             ScalarFunction function,
                                             bool           deterministic = false)
        -> Result<ScalarRegistration, Error>;
    // Connection-local SQLite changes counter, not a result owned by a Statement.
    // Read immediately after the target INSERT, UPDATE or DELETE reaches Done.
    // A later DML can overwrite it; SELECT, DDL, errors and resets do not
    // establish a count for that operation.
    // Trigger, foreign-key and REPLACE auxiliary changes are excluded. A count
    // inside a transaction does not imply that the transaction will commit.
    auto last_changes() -> Result<u64, Error>;
    // Batches are not implicitly atomic; use a Transaction when all statements must commit together.
    auto execute_batch(ref<str> sql) -> Result<empty, Error>;
    auto idle() -> bool;
    auto begin(TransactionMode mode = TransactionMode::Deferred) -> Result<Transaction, Error>;

    // A snapshot observes the current run without retaining the connection.
    auto cancellation() const -> Option<Cancellation>;

    // Synchronous callback; keep all statements, blobs and transactions inside its
    // scope. Cancellation is cooperative and does not undo a completed commit.
    template<class F>
    auto run(const Cancellation& cancel, F&& function) -> decltype(function(*this)) {
        auto scope = enter_run(cancel);
        if (scope.is_err()) return Err(rstd::move(scope).unwrap_err());
        return function(*this);
    }

private:
    static auto prepare_owned(const Arc<Database>& database_,
                              ref<str>             sql,
                              unsigned long long   token) -> Result<Statement, Error>;
};

class Transaction {
    friend class Connection;
    Arc<Database>      database_;
    unsigned long long token_ {};
    Transaction(Arc<Database> db, unsigned long long token);
    auto finish(const char* sql) -> Result<empty, Error>;
    void release();

public:
    Transaction(const Transaction&)                    = delete;
    auto operator=(const Transaction&) -> Transaction& = delete;
    Transaction(Transaction&& other) noexcept;
    auto operator=(Transaction&& other) -> Transaction&;
    ~Transaction();
    auto cancellation() const -> Option<Cancellation>;
    auto prepare(ref<str> sql) -> Result<Statement, Error>;
    // Close before commit. Rollback/drop invalidate I/O, but close remains available.
    auto open_blob(ref<str> schema,
                   ref<str> table,
                   ref<str> column,
                   i64      row_id,
                   BlobMode mode = BlobMode::ReadOnly) -> Result<Blob, Error>;
    // Same connection-local counter and timing requirements as Connection::last_changes().
    auto last_changes() -> Result<u64, Error>;
    auto commit() -> Result<empty, Error>;
    auto rollback() -> Result<empty, Error>;
    auto execute_batch(ref<str> sql) -> Result<empty, Error>;
};

} // namespace sqlito

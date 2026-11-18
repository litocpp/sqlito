export module sqlito:executor;
import :connection;
import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::sync::Arc;
using rstd::async::oneshot::Receiver;

namespace sqlito
{
using Completion = alloc::boxed::Box<dyn<FnMut<void()>>>;
using Work       = alloc::boxed::Box<dyn<FnMut<Completion(Connection&)>>>;
struct WorkerState;
} // namespace sqlito

export namespace sqlito
{
template<class T>
class Operation {
    Receiver<Result<T, Error>> result_;
    Cancellation               cancellation_;
    bool                       active_ { true };

public:
    using Output = Result<T, Error>;
    Operation(Receiver<Output> result, Cancellation cancellation)
        : result_(rstd::move(result)), cancellation_(rstd::move(cancellation)) {}
    Operation(const Operation&)                    = delete;
    auto operator=(const Operation&) -> Operation& = delete;
    Operation(Operation&& other) noexcept
        : result_(rstd::move(other.result_)),
          cancellation_(rstd::move(other.cancellation_)),
          active_(rstd::exchange(other.active_, false)) {}
    auto operator=(Operation&&) -> Operation& = delete;
    ~Operation() {
        if (active_) cancellation_.cancel();
    }
    void cancel() const { cancellation_.cancel(); }
    auto poll(mut_ref<Operation> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        auto& value = *self;
        auto  ready = rstd::future::poll(value.result_, cx);
        if (ready.is_pending()) return rstd::task::Poll<Output>::Pending();
        value.active_ = false;
        auto result   = rstd::move(ready).take();
        if (result.is_err())
            return rstd::task::Poll<Output>::Ready(
                Err(error(ErrorKind::State, "worker stopped without result"_str)));
        return rstd::task::Poll<Output>::Ready(rstd::move(result).unwrap());
    }
};

class ExecutorHandle {
    Arc<WorkerState> state_;
    auto             enqueue(Work&& run, const Cancellation& cancel) -> Result<empty, Error>;

public:
    explicit ExecutorHandle(Arc<WorkerState> state);
    ExecutorHandle(ExecutorHandle&&) noexcept;
    auto operator=(ExecutorHandle&&) noexcept -> ExecutorHandle&;
    ~ExecutorHandle();
    auto clone() const -> ExecutorHandle;
    // Capture owned inputs; return owned data, never session handles or borrowed
    // views.
    template<class F>
    auto submit(F function)
        -> Result<Operation<typename decltype(function(rstd::declval<Connection&>()))::value_type>,
                  Error> {
        using Output  = decltype(function(rstd::declval<Connection&>()));
        using T       = typename Output::value_type;
        auto pair     = rstd::async::oneshot::channel<Output>();
        auto cancel   = Cancellation::make();
        auto run      = Work::make([function = rstd::move(function),
                                    cancel   = cancel.clone(),
                                    sender   = rstd::move(pair.template get<0>())](
                                       Connection& db) mutable -> Completion {
            auto result = db.run(cancel, function);
            if (! db.idle())
                result = Err(error(ErrorKind::State, "operation retained database resources"_str));
            return Completion::make(
                [sender = rstd::move(sender), result = rstd::move(result)]() mutable {
                    (void)sender.send(rstd::move(result));
                });
        });
        auto admitted = enqueue(rstd::move(run), cancel);
        if (admitted.is_err()) return Err(rstd::move(admitted).unwrap_err());
        return Ok(Operation<T> { rstd::move(pair.template get<1>()), rstd::move(cancel) });
    }
};

// Own this on the management thread; await shutdown before destroying it.
// Worker callbacks may capture handles, but must not own their executor.
class Executor {
    Arc<WorkerState>                        state_;
    Option<rstd::thread::JoinHandle<empty>> thread_;
    Receiver<Result<empty, Error>>          exited_;
    Executor(Arc<WorkerState>                state,
             rstd::thread::JoinHandle<empty> thread,
             Receiver<Result<empty, Error>>  exited);

public:
    Executor(const Executor&)                    = delete;
    auto operator=(const Executor&) -> Executor& = delete;
    Executor(Executor&& other) noexcept;
    auto operator=(Executor&&) -> Executor& = delete;
    ~Executor();
    static auto open(ref<str> path, usize capacity = usize(64)) -> Result<Executor, Error>;
    auto        handle() const -> ExecutorHandle;
    void        request_stop();
    auto        shutdown() -> coro<Result<empty, Error>>;
};
} // namespace sqlito

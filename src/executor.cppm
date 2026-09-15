module;
#include <rstd/macro.hpp>
export module sqlito:executor;
import :connection;
import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::sync::Arc;
using rstd::sync::Mutex;
using Condvar = rstd::sync::Condvar;
using rstd::async::oneshot::Sender;
using rstd::async::oneshot::Receiver;

namespace sqlito
{
using Completion = alloc::boxed::Box<dyn<FnMut<void()>>>;
using Work       = alloc::boxed::Box<dyn<FnMut<Completion(Connection&)>>>;
struct WorkItem {
    Work         run;
    Cancellation cancel;
};
struct Queue {
    Vec<Option<WorkItem>> slots;
    usize                 head {}, tail {}, pending {}, outstanding {};
    bool                  stopping {};
    Option<Cancellation>  active;
    explicit Queue(usize capacity) {
        for (usize i {}; i < capacity; ++i) slots.push(None());
    }
};
struct WorkerState {
    Mutex<Queue> queue;
    Condvar      wake;
    explicit WorkerState(usize capacity): queue(Queue { capacity }) {}
    void stop() {
        {
            auto q      = queue.lock().unwrap();
            q->stopping = true;
            if (q->active.is_some()) q->active->cancel();
            for (auto& slot : q->slots)
                if (slot.is_some()) slot->cancel.cancel();
        }
        wake.notify_one();
    }
};
auto worker(Arc<WorkerState> state, Connection db, Sender<Result<empty, Error>> exited) -> empty {
    for (;;) {
        Option<WorkItem> item;
        {
            auto q = state->queue.lock().unwrap();
            state->wake.wait_while(q, [](const Queue& value) {
                return value.pending == usize() && ! value.stopping;
            });
            if (q->pending == usize()) break;
            item    = q->slots[q->head].take();
            q->head = (q->head + usize(1)) % q->slots.len();
            q->pending -= usize(1);
            q->active = Some(item->cancel.clone());
        }
        auto completed = item->run->operator()(db);
        item           = None();
        if (! db.idle()) state->stop();
        {
            auto q    = state->queue.lock().unwrap();
            q->active = None();
            q->outstanding -= usize(1);
        }
        completed->operator()();
    }
    auto closed = db.close();
    (void)exited.send(rstd::move(closed));
    return empty {};
}
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

public:
    explicit ExecutorHandle(Arc<WorkerState> state): state_(rstd::move(state)) {}
    auto clone() const -> ExecutorHandle { return ExecutorHandle { state_.clone() }; }
    // Capture owned inputs; return owned data, never session handles or borrowed
    // views.
    template<class F>
    auto submit(F function)
        -> Result<Operation<typename decltype(function(rstd::declval<Connection&>()))::value_type>,
                  Error> {
        using Output = decltype(function(rstd::declval<Connection&>()));
        using T      = typename Output::value_type;
        auto pair    = rstd::async::oneshot::channel<Output>();
        auto cancel  = Cancellation::make();
        auto run     = Work::make([function = rstd::move(function),
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
        {
            auto q = state_->queue.lock().unwrap();
            if (q->stopping) return Err(error(ErrorKind::State, "executor is stopping"_str));
            if (q->outstanding == q->slots.len())
                return Err(error(ErrorKind::Busy, "executor capacity exhausted"_str));
            q->slots[q->tail] = Some(WorkItem { rstd::move(run), cancel.clone() });
            q->tail           = (q->tail + usize(1)) % q->slots.len();
            q->pending += usize(1);
            q->outstanding += usize(1);
        }
        state_->wake.notify_one();
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
             Receiver<Result<empty, Error>>  exited)
        : state_(rstd::move(state)),
          thread_(Some(rstd::move(thread))),
          exited_(rstd::move(exited)) {}

public:
    Executor(const Executor&)                    = delete;
    auto operator=(const Executor&) -> Executor& = delete;
    Executor(Executor&& other) noexcept
        : state_(rstd::move(other.state_)),
          thread_(other.thread_.take()),
          exited_(rstd::move(other.exited_)) {}
    auto operator=(Executor&&) -> Executor& = delete;
    ~Executor() {
        if (thread_.is_some()) {
            state_->stop();
            (void)rstd::move(thread_).unwrap().join();
        }
    }
    static auto open(ref<str> path, usize capacity = usize(64)) -> Result<Executor, Error> {
        if (capacity == usize())
            return Err(error(ErrorKind::Input, "capacity must be positive"_str));
        auto opened = rstd_try(Connection::open(path));
        auto state  = Arc<WorkerState>::make(capacity);
        auto pair   = rstd::async::oneshot::channel<Result<empty, Error>>();
        auto thread = rstd::thread::spawn([state  = state.clone(),
                                           db     = rstd::move(opened),
                                           exited = rstd::move(pair.get<0>())]() mutable {
            return worker(rstd::move(state), rstd::move(db), rstd::move(exited));
        });
        if (thread.is_err())
            return Err(error(ErrorKind::State, "cannot start database worker"_str));
        return Ok(
            Executor { rstd::move(state), rstd::move(thread).unwrap(), rstd::move(pair.get<1>()) });
    }
    auto handle() const -> ExecutorHandle { return ExecutorHandle { state_.clone() }; }
    void request_stop() { state_->stop(); }
    auto shutdown() -> coro<Result<empty, Error>> {
        if (thread_.is_none()) co_return Ok(empty {});
        request_stop();
        auto result = co_await exited_;
        auto thread = thread_.take().unwrap();
        auto joined = rstd::move(thread).join();
        if (joined.is_err() || result.is_err())
            co_return Err(error(ErrorKind::State, "database worker failed to stop"_str));
        co_return rstd::move(result).unwrap();
    }
};
} // namespace sqlito

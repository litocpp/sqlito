module;
#include <rstd/macro.hpp>
module sqlito;
import :executor;
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

ExecutorHandle::ExecutorHandle(Arc<WorkerState> state): state_(rstd::move(state)) {
}
ExecutorHandle::ExecutorHandle(ExecutorHandle&&) noexcept                    = default;
auto ExecutorHandle::operator=(ExecutorHandle&&) noexcept -> ExecutorHandle& = default;
ExecutorHandle::~ExecutorHandle()                                            = default;

auto ExecutorHandle::clone() const -> ExecutorHandle {
    return ExecutorHandle { state_.clone() };
}

auto ExecutorHandle::enqueue(Work&& run, const Cancellation& cancel) -> Result<empty, Error> {
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
    return Ok(empty {});
}

Executor::Executor(Arc<WorkerState>                state,
                   rstd::thread::JoinHandle<empty> thread,
                   Receiver<Result<empty, Error>>  exited)
    : state_(rstd::move(state)), thread_(Some(rstd::move(thread))), exited_(rstd::move(exited)) {
}

Executor::Executor(Executor&& other) noexcept
    : state_(rstd::move(other.state_)),
      thread_(other.thread_.take()),
      exited_(rstd::move(other.exited_)) {
}

Executor::~Executor() {
    if (thread_.is_some()) {
        state_->stop();
        (void)rstd::move(thread_).unwrap().join();
    }
}

auto Executor::open(ref<str> path, usize capacity) -> Result<Executor, Error> {
    if (capacity == usize()) return Err(error(ErrorKind::Input, "capacity must be positive"_str));
    auto opened = rstd_try(Connection::open(path));
    auto state  = Arc<WorkerState>::make(capacity);
    auto pair   = rstd::async::oneshot::channel<Result<empty, Error>>();
    auto thread = rstd::thread::spawn([state  = state.clone(),
                                       db     = rstd::move(opened),
                                       exited = rstd::move(pair.get<0>())]() mutable {
        return worker(rstd::move(state), rstd::move(db), rstd::move(exited));
    });
    if (thread.is_err()) return Err(error(ErrorKind::State, "cannot start database worker"_str));
    return Ok(
        Executor { rstd::move(state), rstd::move(thread).unwrap(), rstd::move(pair.get<1>()) });
}

auto Executor::handle() const -> ExecutorHandle {
    return ExecutorHandle { state_.clone() };
}

void Executor::request_stop() {
    state_->stop();
}

auto Executor::shutdown() -> coro<Result<empty, Error>> {
    if (thread_.is_none()) co_return Ok(empty {});
    request_stop();
    auto result = co_await exited_;
    auto thread = thread_.take().unwrap();
    auto joined = rstd::move(thread).join();
    if (joined.is_err() || result.is_err())
        co_return Err(error(ErrorKind::State, "database worker failed to stop"_str));
    co_return rstd::move(result).unwrap();
}
} // namespace sqlito

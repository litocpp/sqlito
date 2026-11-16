module;
#include <sqlite3.h>
#include <climits>
export module sqlito:memory;
export import :error;
using namespace rstd::prelude;
using namespace rstd::literals;
using rstd::alloc::Layout;
using rstd::alloc::PoolSizeClasses;
using rstd::alloc::SharedPoolAllocator;
using rstd::alloc::VirtualMemoryAllocator;
using rstd::sync::Arc;
using rstd::sync::Weak;
using rstd::sync::Mutex;
using rstd::sync::atomic::Atomic;
using rstd::sync::atomic::Ordering;

export namespace sqlito
{
struct MemoryOptions {
    rstd::alloc::PoolOptions pool { .slab_bytes             = usize(262144),
                                    .max_pooled_block_bytes = usize(131072),
                                    .retained_bytes         = usize(1048576),
                                    .size_classes           = PoolSizeClasses::Fine,
                                    .target_blocks_per_slab = usize(4),
                                    .min_slab_bytes         = usize(16384) };
};
struct MemoryAdapterStatistics {
    usize moved_reallocations {}, copied_bytes {};
};
struct MemoryStatistics {
    rstd::alloc::PoolStatistics          pool;
    rstd::alloc::VirtualMemoryStatistics upstream;
    usize                                control_bytes {}, control_layout_bytes {};
    // Requested Global Arc layout bytes, not the system allocator's usable size.
    usize                   owner_bytes {};
    MemoryAdapterStatistics adapter;
};
} // namespace sqlito

namespace sqlito
{
using MemoryPool = SharedPoolAllocator<VirtualMemoryAllocator>;
struct MemoryState;
struct MemoryRegistry {
    MemoryState*              active {};
    Option<Weak<MemoryState>> owner;
};
Mutex<MemoryRegistry> memory_registry;
Atomic<MemoryState*>  memory_target { nullptr };

struct MemoryState {
    MemoryPool          pool;
    sqlite3_mem_methods previous {};
    usize               leases {};
    usize               control_bytes, control_layout_bytes;
    Atomic<usize>       moved_reallocations {}, copied_bytes {};
    explicit MemoryState(MemoryPool value, usize mapped, usize layout)
        : pool(rstd::move(value)), control_bytes(mapped), control_layout_bytes(layout) {}
    auto shutdown(MemoryRegistry& registry) noexcept -> int {
        if (registry.active != this) return SQLITE_OK;
        int code = sqlite3_shutdown();
        if (code != SQLITE_OK) return code;
        code = sqlite3_config(SQLITE_CONFIG_MALLOC, &previous);
        if (code != SQLITE_OK) return code;
        memory_target.store(nullptr, Ordering::Release);
        registry.active = nullptr;
        registry.owner  = None();
        return SQLITE_OK;
    }
    ~MemoryState() {
        auto registry = memory_registry.lock().unwrap_unchecked();
        (void)shutdown(*registry);
    }
};

auto memory_layout(usize size) noexcept -> Layout {
    return Layout::from_size_align_unchecked(size, usize(16));
}
auto memory_roundup(int size) noexcept -> int {
    if (size <= 0) return 0;
    auto layout = Layout::from_size_align(usize(size), usize(16));
    if (layout.is_none()) return 0;
    auto* state = memory_target.load(Ordering::Acquire);
    if (! state) return 0;
    auto rounded = state->pool.good_size(*layout);
    return rounded >= usize(size) && rounded <= usize(INT_MAX)
               ? static_cast<int>(rounded.to_primitive())
               : 0;
}
auto memory_malloc(int size) noexcept -> void* {
    if (size <= 0) return nullptr;
    auto layout = Layout::from_size_align(usize(size), usize(16));
    if (layout.is_none()) return nullptr;
    auto* state = memory_target.load(Ordering::Acquire);
    if (! state) return nullptr;
    auto result = state->pool.allocate(*layout);
    return result.is_ok() ? result->pointer : nullptr;
}
void memory_free(void* pointer) noexcept {
    if (! pointer) return;
    auto* state = memory_target.load(Ordering::Acquire);
    state->pool.deallocate(pointer, memory_layout(state->pool.allocation_size(pointer)));
}
auto memory_realloc(void* pointer, int size) noexcept -> void* {
    if (! pointer) return memory_malloc(size);
    if (size <= 0) {
        memory_free(pointer);
        return nullptr;
    }
    auto new_layout = Layout::from_size_align(usize(size), usize(16));
    if (new_layout.is_none()) return nullptr;
    auto* state        = memory_target.load(Ordering::Acquire);
    auto  old_layout   = memory_layout(state->pool.allocation_size(pointer));
    auto  old_capacity = state->pool.usable_size(pointer);
    if (new_layout->size() > old_capacity) {
        auto result = state->pool.allocate(*new_layout);
        if (result.is_err()) return nullptr;
        // SQLite may use the full xSize capacity after an in-place shrink.
        rstd::mem::memcpy(result->pointer, pointer, old_capacity);
        state->pool.deallocate(pointer, old_layout);
        state->copied_bytes.fetch_add(old_capacity, Ordering::Relaxed);
        state->moved_reallocations.fetch_add(usize(1), Ordering::Relaxed);
        return result->pointer;
    }
    auto result = new_layout->size() >= old_layout.size()
                      ? state->pool.grow(pointer, old_layout, *new_layout)
                      : state->pool.shrink(pointer, old_layout, *new_layout);
    return result.is_ok() ? result->pointer : nullptr;
}
auto memory_size(void* pointer) noexcept -> int {
    auto* state = memory_target.load(Ordering::Acquire);
    auto  size  = state->pool.usable_size(pointer);
    return size <= usize(INT_MAX) ? static_cast<int>(size.to_primitive()) : 0;
}
auto memory_init(void*) noexcept -> int {
    return SQLITE_OK;
}
void memory_shutdown(void*) noexcept {
}

class MemoryLease {
    Option<Arc<MemoryState>> state_;

public:
    MemoryLease() = default;
    explicit MemoryLease(Arc<MemoryState> state): state_(Some(rstd::move(state))) {}
    MemoryLease(const MemoryLease&)                    = delete;
    auto operator=(const MemoryLease&) -> MemoryLease& = delete;
    MemoryLease(MemoryLease&& other) noexcept: state_(other.state_.take()) {}
    auto operator=(MemoryLease&& other) noexcept -> MemoryLease& {
        if (this != &other) {
            release();
            state_ = other.state_.take();
        }
        return *this;
    }
    void release() noexcept {
        auto state = state_.take();
        if (state.is_none()) return;
        {
            auto registry = memory_registry.lock().unwrap_unchecked();
            --(*state)->leases;
        }
    }
    ~MemoryLease() { release(); }
};
struct OpenedDatabase {
    sqlite3*    handle;
    MemoryLease memory;
};
auto open_database(const char* path, int flags) -> Result<OpenedDatabase, Error> {
    MemoryLease lease;
    auto        registry = memory_registry.lock().unwrap_unchecked();
    if (registry->active) {
        auto state = registry->owner->upgrade();
        if (! state)
            return Err(error(ErrorKind::Busy, "SQLite memory runtime is shutting down"_str));
        ++state->leases;
        lease = MemoryLease(rstd::move(state));
    }
    sqlite3* handle = nullptr;
    int      code   = sqlite3_open_v2(path, &handle, flags, nullptr);
    if (code != SQLITE_OK) {
        auto failure = database_error(handle, code);
        if (handle) (void)sqlite3_close(handle);
        return Err(rstd::move(failure));
    }
    return Ok(OpenedDatabase { handle, rstd::move(lease) });
}
} // namespace sqlito

export namespace sqlito
{
// Owns SQLite initialization; all SQLite handles must be opened through sqlito.
class MemoryRuntime {
    Arc<MemoryState> state_;
    explicit MemoryRuntime(Arc<MemoryState> state): state_(rstd::move(state)) {}

public:
    MemoryRuntime(const MemoryRuntime&)                               = delete;
    auto operator=(const MemoryRuntime&) -> MemoryRuntime&            = delete;
    MemoryRuntime(MemoryRuntime&&) noexcept                           = default;
    auto        operator=(MemoryRuntime&&) noexcept -> MemoryRuntime& = default;
    static auto create(MemoryOptions options = {}) -> Result<MemoryRuntime, Error> {
        if (Layout::from_size_align(usize(), options.pool.slab_granularity).is_none() ||
            (options.pool.size_classes != PoolSizeClasses::Fine &&
             options.pool.size_classes != PoolSizeClasses::PowersOfTwo))
            return Err(error(ErrorKind::Input, "invalid SQLite pool options"_str));
        auto control                  = VirtualMemoryAllocator {};
        options.pool.slab_granularity = options.pool.slab_granularity.max(control.page_size());
        auto control_layout           = MemoryPool::control_layout<VirtualMemoryAllocator>();
        if (control_layout.is_none())
            return Err(error(ErrorKind::Range, "invalid SQLite pool control layout"_str));
        auto control_bytes = control.allocation_size(*control_layout);
        if (control_bytes.is_none())
            return Err(error(ErrorKind::Range, "invalid SQLite pool control mapping"_str));
        auto pool =
            MemoryPool::try_make_in(rstd::move(control), options.pool, VirtualMemoryAllocator {});
        if (pool.is_err())
            return Err(Error::from(ErrorKind::Database,
                                   SQLITE_NOMEM,
                                   String::make("cannot allocate SQLite memory pool"_str)));
        auto state = Arc<MemoryState>::try_make(
            rstd::move(pool).unwrap(), *control_bytes, control_layout->size());
        if (state.is_err())
            return Err(Error::from(ErrorKind::Database,
                                   SQLITE_NOMEM,
                                   String::make("cannot allocate SQLite memory runtime"_str)));
        auto owner    = rstd::move(state).unwrap();
        auto registry = memory_registry.lock().unwrap_unchecked();
        if (registry->active)
            return Err(error(ErrorKind::Busy, "SQLite memory runtime is already installed"_str));
        int code = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &owner->previous);
        if (code != SQLITE_OK)
            return Err(error(ErrorKind::State, "SQLite is already initialized"_str));
        sqlite3_mem_methods methods { memory_malloc,  memory_free, memory_realloc,  memory_size,
                                      memory_roundup, memory_init, memory_shutdown, &*owner };
        code = sqlite3_config(SQLITE_CONFIG_MALLOC, &methods);
        if (code != SQLITE_OK) return Err(database_error(nullptr, code));
        registry->active = &*owner;
        registry->owner  = Some(owner.downgrade());
        memory_target.store(&*owner, Ordering::Release);
        code = sqlite3_initialize();
        if (code != SQLITE_OK) {
            (void)owner->shutdown(*registry);
            return Err(database_error(nullptr, code));
        }
        return Ok(MemoryRuntime(rstd::move(owner)));
    }
    auto shutdown() -> Result<empty, Error> {
        auto registry = memory_registry.lock().unwrap_unchecked();
        if (state_->leases != usize())
            return Err(error(ErrorKind::Busy, "SQLite connections are still alive"_str));
        int code = state_->shutdown(*registry);
        if (code != SQLITE_OK) return Err(database_error(nullptr, code));
        return Ok(empty {});
    }
    auto snapshot() const -> MemoryStatistics {
        auto pool         = state_->pool.snapshot();
        auto owner_layout = Arc<MemoryState>::allocation_layout_in<::alloc::Global>();
        return { pool.pool,
                 pool.upstream,
                 state_->control_bytes,
                 state_->control_layout_bytes,
                 owner_layout->size(),
                 { state_->moved_reallocations.load(Ordering::Relaxed),
                   state_->copied_bytes.load(Ordering::Relaxed) } };
    }
    auto trim(usize retain = {}) const -> rstd::alloc::PoolTrimReport {
        return state_->pool.trim(retain);
    }
};
} // namespace sqlito

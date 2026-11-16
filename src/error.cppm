module;
#include <sqlite3.h>
#include <rstd/enum.hpp>
export module sqlito:error;
export import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;

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
class Error {
    RSTD_ENUM(Error,
              (Database, (int code; String message;)),
              (State, (int code; String message;)),
              (Input, (int code; String message;)),
              (Unbound, (int code; String message;)),
              (Type, (int code; String message;)),
              (Range, (int code; String message;)),
              (Cancelled, (int code; String message;)),
              (Busy, (int code; String message;)),
              (Callback, (ErrorKind kind; int code; String message; CallbackFailure failure;)))

public:
    static auto from(ErrorKind kind, int code, String message) -> Error {
        switch (kind) {
        case ErrorKind::Database: return Error::Database(code, rstd::move(message));
        case ErrorKind::State: return Error::State(code, rstd::move(message));
        case ErrorKind::Input: return Error::Input(code, rstd::move(message));
        case ErrorKind::Unbound: return Error::Unbound(code, rstd::move(message));
        case ErrorKind::Type: return Error::Type(code, rstd::move(message));
        case ErrorKind::Range: return Error::Range(code, rstd::move(message));
        case ErrorKind::Cancelled: return Error::Cancelled(code, rstd::move(message));
        case ErrorKind::Busy: return Error::Busy(code, rstd::move(message));
        }
        return Error::State(code, rstd::move(message));
    }
    static auto with_callback(Error error, CallbackFailure failure) -> Error {
        auto kind    = error.kind();
        auto code    = error.code();
        auto message = String::make(error.message());
        return Error::Callback(kind, code, rstd::move(message), rstd::move(failure));
    }
    auto kind() const -> ErrorKind {
        RSTD_MATCH(*this) {
            RSTD_CASE(Database, code, message) {
                return ErrorKind::Database;
            }
            RSTD_CASE(State, code, message) {
                return ErrorKind::State;
            }
            RSTD_CASE(Input, code, message) {
                return ErrorKind::Input;
            }
            RSTD_CASE(Unbound, code, message) {
                return ErrorKind::Unbound;
            }
            RSTD_CASE(Type, code, message) {
                return ErrorKind::Type;
            }
            RSTD_CASE(Range, code, message) {
                return ErrorKind::Range;
            }
            RSTD_CASE(Cancelled, code, message) {
                return ErrorKind::Cancelled;
            }
            RSTD_CASE(Busy, code, message) {
                return ErrorKind::Busy;
            }
            RSTD_CASE(Callback, kind, code, message, failure) {
                return kind;
            }
        }
        return ErrorKind::State;
    }
    auto code() const -> int {
        RSTD_MATCH(*this) {
            RSTD_CASE(Database, code, message) {
                return code;
            }
            RSTD_CASE(State, code, message) {
                return code;
            }
            RSTD_CASE(Input, code, message) {
                return code;
            }
            RSTD_CASE(Unbound, code, message) {
                return code;
            }
            RSTD_CASE(Type, code, message) {
                return code;
            }
            RSTD_CASE(Range, code, message) {
                return code;
            }
            RSTD_CASE(Cancelled, code, message) {
                return code;
            }
            RSTD_CASE(Busy, code, message) {
                return code;
            }
            RSTD_CASE(Callback, kind, code, message, failure) {
                return code;
            }
        }
        return 0;
    }
    auto message() const -> ref<str> {
        RSTD_MATCH(*this) {
            RSTD_CASE(Database, code, message) {
                return message.as_str();
            }
            RSTD_CASE(State, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Input, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Unbound, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Type, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Range, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Cancelled, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Busy, code, message) {
                return message.as_str();
            }
            RSTD_CASE(Callback, kind, code, message, failure) {
                return message.as_str();
            }
        }
        return ""_str;
    }
    auto callback() const -> Option<CallbackFailure> {
        RSTD_MATCH(*this) {
            RSTD_CASE(Callback, kind, code, message, failure) {
                return Some(
                    CallbackFailure { failure.kind, failure.code, failure.message.clone() });
            }
            RSTD_CASE(Database, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(State, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Input, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Unbound, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Type, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Range, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Cancelled, code, message) {
                return Option<CallbackFailure>(None());
            }
            RSTD_CASE(Busy, code, message) {
                return Option<CallbackFailure>(None());
            }
        }
        return Option<CallbackFailure>(None());
    }
};
} // namespace sqlito

namespace rstd
{
export template<>
struct Impl<fmt::Display, sqlito::Error> : ImplBase<sqlito::Error> {
    auto fmt(fmt::Formatter& formatter) const -> bool {
        return formatter.write_str(this->self().message());
    }
};

export template<>
struct Impl<fmt::Debug, sqlito::Error> : ImplBase<sqlito::Error> {
    auto fmt(fmt::Formatter& formatter) const -> bool {
        const auto& error = this->self();
        return formatter.write_fmt(
            fmt::Arguments::make("sqlito::Error {{ kind: {}, code: {}, message: {:?} }}",
                                 static_cast<int>(error.kind()),
                                 error.code(),
                                 error.message()));
    }
};

export template<>
struct Impl<error::Error, sqlito::Error> : DefaultInImpl<error::Error, sqlito::Error> {
    auto source() const noexcept -> Option<error::ErrorRef> { return None(); }
};
} // namespace rstd

namespace sqlito
{
auto error(ErrorKind kind, ref<str> message) -> Error {
    return Error::from(kind, 0, String::make(message));
}
auto database_error(sqlite3* db, int code) -> Error {
    auto bytes =
        rstd::ffi::CStr::from_ptr(db ? sqlite3_errmsg(db) : sqlite3_errstr(code)).to_bytes();
    auto text = rstd::str_::from_utf8(bytes);
    return Error::from((code & 255) == SQLITE_INTERRUPT ? ErrorKind::Cancelled
                                                        : ErrorKind::Database,
                       code,
                       String::make(text.is_ok() ? *text : "SQLite error"_str));
}
} // namespace sqlito

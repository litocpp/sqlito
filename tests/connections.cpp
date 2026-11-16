#include <rstd/test/gtest.hpp>
import sqlito;
import rstd.test;
using namespace rstd::prelude;
using namespace rstd::literals;

namespace
{
auto database_path(const rstd::test::TempDir& directory) -> rstd::path::PathBuf {
    auto path = rstd::path::PathBuf::from(directory.path());
    path.push(rstd::path::PathBuf::from("connections.sqlite"_str).as_path());
    return path;
}
template<class Session>
auto stored_value(Session& session) -> i64 {
    auto query = session.prepare("SELECT value FROM settings"_str).unwrap();
    EXPECT_EQ(query.step().unwrap(), sqlito::Step::Row);
    return *query.integer(usize()).unwrap();
}
} // namespace

TEST(Connections, DefaultModeCreatesWritableDatabase) {
    auto directory  = rstd::test::TempDir::make().unwrap();
    auto path       = database_path(directory);
    auto name       = path.as_path().to_str().unwrap();
    auto connection = sqlito::Connection::open(name).unwrap();
    ASSERT_TRUE(connection
                    .execute_batch("CREATE TABLE settings(value INTEGER);"
                                   "INSERT INTO settings VALUES(41);"_str)
                    .is_ok());
    EXPECT_EQ(stored_value(connection), i64(41));
    ASSERT_TRUE(connection.close().is_ok());
    EXPECT_TRUE(rstd::fs::metadata(path.as_path()).is_ok());
}

TEST(Connections, ReadOnlyReadsAndRejectsWrites) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = database_path(directory);
    auto name      = path.as_path().to_str().unwrap();
    {
        auto writer = sqlito::Connection::open(name).unwrap();
        ASSERT_TRUE(writer
                        .execute_batch("CREATE TABLE settings(value INTEGER);"
                                       "INSERT INTO settings VALUES(41);"_str)
                        .is_ok());
    }
    auto before = rstd::fs::read(path.as_path()).unwrap();
    auto reader = sqlito::Connection::open(name, sqlito::OpenMode::ReadOnly).unwrap();
    ASSERT_TRUE(reader.execute_batch("PRAGMA trusted_schema=OFF"_str).is_ok());
    EXPECT_EQ(stored_value(reader), i64(41));
    auto write = reader.execute_batch("UPDATE settings SET value=42"_str);
    ASSERT_TRUE(write.is_err());
    EXPECT_EQ(write.unwrap_err().kind(), sqlito::ErrorKind::Database);
    EXPECT_EQ(stored_value(reader), i64(41));
    EXPECT_EQ(rstd::fs::read(path.as_path()).unwrap(), before);
}

TEST(Connections, ModesWithoutCreateDoNotCreateMissingFiles) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = database_path(directory);
    auto name      = path.as_path().to_str().unwrap();
    EXPECT_TRUE(sqlito::Connection::open(name, sqlito::OpenMode::ReadOnly).is_err());
    EXPECT_TRUE(rstd::fs::metadata(path.as_path()).is_err());
    EXPECT_TRUE(sqlito::Connection::open(name, sqlito::OpenMode::ReadWrite).is_err());
    EXPECT_TRUE(rstd::fs::metadata(path.as_path()).is_err());
}

TEST(Connections, ReadWriteOpensExistingDatabaseWithoutCreate) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = database_path(directory);
    auto name      = path.as_path().to_str().unwrap();
    {
        auto created = sqlito::Connection::open(name).unwrap();
        ASSERT_TRUE(created
                        .execute_batch("CREATE TABLE settings(value INTEGER);"
                                       "INSERT INTO settings VALUES(41);"_str)
                        .is_ok());
    }
    auto writer = sqlito::Connection::open(name, sqlito::OpenMode::ReadWrite).unwrap();
    ASSERT_TRUE(writer.execute_batch("UPDATE settings SET value=42"_str).is_ok());
    EXPECT_EQ(stored_value(writer), i64(42));
}

TEST(Connections, InvalidOpenModeIsRejectedBeforeFileCreation) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = database_path(directory);
    auto result    = sqlito::Connection::open(path.as_path().to_str().unwrap(),
                                              static_cast<sqlito::OpenMode>(3));
    ASSERT_TRUE(result.is_err());
    EXPECT_EQ(result.unwrap_err().kind(), sqlito::ErrorKind::Input);
    EXPECT_TRUE(rstd::fs::metadata(path.as_path()).is_err());
}

TEST(Connections, ReadOnlyTransactionKeepsItsConnectionAndSnapshot) {
    auto directory = rstd::test::TempDir::make().unwrap();
    auto path      = database_path(directory);
    auto name      = path.as_path().to_str().unwrap();
    auto writer    = sqlito::Connection::open(name).unwrap();
    ASSERT_TRUE(writer
                    .execute_batch("PRAGMA journal_mode=WAL;"
                                   "CREATE TABLE settings(value INTEGER);"
                                   "INSERT INTO settings VALUES(41);"_str)
                    .is_ok());
    auto transaction = [&] {
        auto reader = sqlito::Connection::open(name, sqlito::OpenMode::ReadOnly).unwrap();
        reader.execute_batch("PRAGMA trusted_schema=OFF"_str).unwrap();
        return reader.begin(sqlito::TransactionMode::Deferred).unwrap();
    }();
    EXPECT_EQ(stored_value(transaction), i64(41));
    ASSERT_TRUE(writer.execute_batch("UPDATE settings SET value=42"_str).is_ok());
    EXPECT_EQ(stored_value(transaction), i64(41));
    auto write = transaction.execute_batch("UPDATE settings SET value=43"_str);
    EXPECT_TRUE(write.is_err());
    ASSERT_TRUE(transaction.rollback().is_ok());
    auto current = sqlito::Connection::open(name, sqlito::OpenMode::ReadOnly).unwrap();
    EXPECT_EQ(stored_value(current), i64(42));
}

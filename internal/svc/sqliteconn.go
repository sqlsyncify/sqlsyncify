package svc

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func NewSqliteConn(site string) (*sql.DB, error) {
	// A workaround is to use "file::memory:?cache=shared" (or "file:foobar?mode=memory&cache=shared").
	// Every connection to this string will point to the same in-memory database.
	//dbFile := fmt.Sprintf("file:%s?mode=memory&cache=shared", site)
	dbFile := fmt.Sprintf("./storage/%s.db?cache=shared&mode=memory", site)
	sqliteConn, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	// 启用WAL模式（Write-Ahead Logging）
	// WAL模式可以显著提高SQLite的并发性能，特别是在高并发读写场景下。通过启用WAL模式，可以减少锁库的概率，从而提高写入操作的效率
	_, err = sqliteConn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		log.Println("sqliteConn Exec WAL error:", err)
	}
	// 默认情况下，PRAGMA synchronous被设置为FULL，这意味着每次数据库修改操作后，SQLite都会等待数据完全写入磁盘，以确保数据在系统崩溃或断电时不会丢失
	// 设置为OFF，SQLite将不再等待数据完全写入磁盘，而是将数据写入缓冲区后立即继续执行后续操作。
	// 这种设置可以显著提高数据库的写入性能，因为减少了磁盘I/O操作的等待时间
	_, err = sqliteConn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		log.Println("sqliteConn Exec sync-OFF error:", err)
	}

	return sqliteConn, nil
}

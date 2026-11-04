package importer

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/utils"
	"strings"
	"sync"
)

type rowsBatch struct {
	Items     []map[string]interface{}
	TableName string
	Cols      []string
}

type rowBatch struct {
	Item      map[string]interface{}
	TableName string
	Cols      []string
}

type readSql struct {
	IsFirst   bool
	ReadSql   string
	TableName string
}

type Config struct {
	ChWriteRow chan *rowBatch
	ChReadSql  chan *readSql
	Db         *sql.DB
	DbLocal    *sql.DB
	Site       string
	Ctx        context.Context
	BatchSize  int
	BatchCore  int
	Debug      bool
	AppConf    config.Config
	SiteConf   *config.SiteConfig
}

type Importer interface {
	Run() error
}

type importerImplement struct {
	cfg   *Config
	mu    sync.Mutex
	extra map[string]*sql.DB
	bad   error
}

func NewImporter(cfg *Config) Importer {
	//载入本地db的批量200-400,不能太大,字段多时insert into会报too many SQL variables
	cfg.BatchSize = 200
	cfg.BatchCore = runtime.NumCPU()
	return &importerImplement{cfg: cfg, extra: map[string]*sql.DB{}}
}

func (i *importerImplement) fail(err error) {
	if err == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.bad == nil {
		i.bad = err
	}
}

func (i *importerImplement) runErr() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.bad
}

func (i *importerImplement) externalDB(name string) (*sql.DB, error) {
	if !utils.SafeDatasourceName(name) {
		return nil, fmt.Errorf("invalid datasource name")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if db, ok := i.extra[name]; ok {
		return db, nil
	}
	db, err := svc.NewDbConn(name)
	if err != nil {
		return nil, err
	}
	i.extra[name] = db
	return db, nil
}

func (i *importerImplement) closeExtra() {
	i.mu.Lock()
	defer i.mu.Unlock()
	for name, db := range i.extra {
		_ = db.Close()
		delete(i.extra, name)
	}
}

func (i *importerImplement) execOnDb(exDB *sql.DB, query string, args ...any) (sql.Result, error) {
	if exDB != nil {
		return exDB.ExecContext(i.cfg.Ctx, query, args...)
	}
	return i.cfg.Db.ExecContext(i.cfg.Ctx, query, args...)
}

func (i *importerImplement) getRowsFromDb(exDB *sql.DB, query string, args ...any) (*sql.Rows, error) {
	// 以SELECT开头的 && 没有limit && 如果开启了limit -> 附加limit在末尾
	sel := utils.IsPrefix(query, "SELECT")
	nolimit := !strings.Contains(utils.RemoveSqlComment(query), "LIMIT ")
	if sel && nolimit && i.cfg.SiteConf.EnabledImportLimit() {
		query += fmt.Sprintf(" LIMIT %d", i.cfg.SiteConf.ImportLimit)
	}
	if exDB != nil {
		return exDB.QueryContext(i.cfg.Ctx, query, args...)
	}
	return i.cfg.Db.QueryContext(i.cfg.Ctx, query, args...)
}

// Run 导入远程mysql数据
// 写入本地db表
func (i *importerImplement) Run() error {
	defer i.closeExtra()
	if !utils.CheckSiteFormat(i.cfg.Site) {
		return fmt.Errorf("invalid site")
	}
	dirPath, err := utils.ResolveUnder("etc/sites", i.cfg.Site, "sql-import")
	if err != nil {
		return err
	}
	if _, err = os.Stat(dirPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("[Import] site:%s does not found", i.cfg.Site)
		}
		return err
	}
	sqlFiles, err := utils.ScanDir(dirPath)
	if err != nil {
		log.Printf("error walking the directory %s: %v\n", dirPath, err)
		return err
	}

	//多核并发读
	i.cfg.ChReadSql = make(chan *readSql, i.cfg.BatchCore)
	var wgRead sync.WaitGroup
	wgRead.Add(i.cfg.BatchCore)
	for c := 1; c < i.cfg.BatchCore+1; c++ {
		go func(workerId int) {
			defer wgRead.Done()
			for item := range i.cfg.ChReadSql {
				if i.cfg.Debug {
					log.Printf("[worker-read-%03d] readSql: %s", workerId, item.ReadSql)
				} else {
					log.Printf("[worker-read-%03d] table: %s", workerId, item.TableName)
				}
				if err1 := i.execSql(item.TableName, item.ReadSql, item.IsFirst); err1 != nil {
					log.Printf("[worker-read-%03d][error] %s", workerId, err1)
					i.fail(err1)
				}
			}
		}(c)
	}

	//多表并发写
	i.cfg.ChWriteRow = make(chan *rowBatch, i.cfg.BatchCore)
	var wgWrite sync.WaitGroup
	wgWrite.Add(i.cfg.BatchCore)
	for c := 1; c < i.cfg.BatchCore+1; c++ {
		go func(workerId int) {
			count := 0
			buf := make(map[string]*rowsBatch, i.cfg.BatchSize)
			defer wgWrite.Done()
			for item := range i.cfg.ChWriteRow {
				count++
				obj, ok := buf[item.TableName]
				if !ok {
					obj = &rowsBatch{TableName: item.TableName, Cols: item.Cols}
					buf[item.TableName] = obj
				}
				obj.Items = append(obj.Items, item.Item)
				if len(obj.Items) == i.cfg.BatchSize {
					if i.cfg.Debug {
						log.Printf("[worker-write-%03d] %s flush count:%d, size:%d\n ", workerId, item.TableName, count, len(obj.Items))
					}
					if err1 := i.insertBatch(obj); err1 != nil {
						i.fail(err1)
					}
					obj.Items = []map[string]any{}
				}
			}
			for _, item := range buf {
				if err1 := i.insertBatch(item); err1 != nil {
					i.fail(err1)
				}
			}
		}(c)
	}

	for _, file := range sqlFiles {
		if i.cfg.Ctx.Err() != nil {
			i.fail(i.cfg.Ctx.Err())
			break
		}
		if i.runErr() != nil {
			break
		}
		if err = i.loadDataFromSqlFile(file); err != nil {
			log.Println(file, err)
			i.fail(err)
			break
		}
	}

	log.Println("load sql files end.")
	close(i.cfg.ChReadSql)
	wgRead.Wait()

	log.Println("waiting for all import workers...")
	close(i.cfg.ChWriteRow)
	wgWrite.Wait()
	if err = i.runErr(); err != nil {
		return err
	}
	i.sqliteReSize()
	return nil
}

// 经常删除数据,回收sqlite文件占用空间
func (i *importerImplement) sqliteReSize() {
	_, err := i.cfg.DbLocal.Exec("VACUUM")
	if err != nil {
		log.Println("sqliteConn Exec VACUUM error:", err)
	}
}

func (i *importerImplement) queryDB(sqlStr string) (*sql.DB, error) {
	ds, ok, err := utils.SQLDirective(sqlStr, "-- ds=")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return i.externalDB(ds)
}

// 提交SQL到远程数据源抽取数据
func (i *importerImplement) loadDataFromSqlFile(file string) error {
	log.Println("Load File:", file)
	sqlf, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("error at load file:%v", err)
	}
	sqlStr := string(sqlf)
	if strings.Contains(sqlStr, "{lang}") {
		if !utils.CheckLangFormat(i.cfg.SiteConf.Lang) {
			return fmt.Errorf("invalid lang")
		}
		sqlStr = strings.ReplaceAll(sqlStr, "{lang}", i.cfg.SiteConf.Lang)
	}

	base := filepath.Base(file)
	dot := strings.Index(base, ".")
	if dot <= 0 {
		return fmt.Errorf("invalid sql filename")
	}
	tableName := base[:dot]
	if !utils.SafeIdent(tableName) {
		return fmt.Errorf("invalid table name %q", tableName)
	}
	if i.cfg.Debug {
		log.Println("sql:", sqlStr)
	}
	log.Println("tableName:", tableName)

	primaryKey := "id"
	if key, ok, err := utils.SQLDirective(sqlStr, "-- key="); err != nil {
		return err
	} else if ok {
		primaryKey = key
	}
	if !utils.SafeIdent(primaryKey) {
		return fmt.Errorf("invalid primary key %q", primaryKey)
	}

	queryDB, err := i.queryDB(sqlStr)
	if err != nil {
		return err
	}

	sqlList := strings.Split(sqlStr, ";")
	sqlListLen := len(sqlList)
	if sqlListLen > 1 {
		for _, sqlItem := range sqlList[0 : sqlListLen-1] {
			if _, err2 := i.execOnDb(queryDB, sqlItem); err2 != nil {
				return fmt.Errorf("sql setup failed: %w", err2)
			}
		}
		sqlStr = sqlList[sqlListLen-1]
	}
	if !utils.IsPrefix(sqlStr, "SELECT") {
		return fmt.Errorf("invalid sql: do not start with SELECT")
	}

	upperSql := strings.ToUpper(sqlStr)
	fromPos := strings.Index(upperSql, "FROM")
	if fromPos < 0 {
		return fmt.Errorf("invalid sql: missing FROM")
	}
	minMaxSql := fmt.Sprintf("SELECT MIN(%s) as minId, MAX(%s) as maxId %s ", primaryKey, primaryKey, sqlStr[fromPos:])
	rowsMinMax, err := i.getRowsFromDb(queryDB, minMaxSql)
	if err != nil {
		return fmt.Errorf("error at min max key:%v", err)
	}
	defer rowsMinMax.Close()

	var minID, maxID sql.NullInt64
	if rowsMinMax.Next() {
		if err = rowsMinMax.Scan(&minID, &maxID); err != nil {
			return fmt.Errorf("error at scan min max key:%v", err)
		}
		log.Printf("minId: %v, maxId: %v", minID.Int64, maxID.Int64)
	} else {
		log.Println("can not get min max key value")
	}
	if err = rowsMinMax.Err(); err != nil {
		return err
	}

	// The first chunk creates the table before any other chunk is queued.
	if !minID.Valid || !maxID.Valid || maxID.Int64-minID.Int64 < 10000 {
		return i.execSql(tableName, sqlStr, true)
	}
	limit := int64(1000)
	first := true
	for id := minID.Int64; id < maxID.Int64; id += limit + 1 {
		betweenSql := sqlStr
		if !strings.Contains(upperSql, "WHERE") {
			betweenSql += " WHERE 1=1"
		}
		betweenSql += fmt.Sprintf(" AND %s between %d and %d", primaryKey, id, id+limit)
		if first {
			if err = i.execSql(tableName, betweenSql, true); err != nil {
				return err
			}
			first = false
			continue
		}
		item := &readSql{ReadSql: betweenSql, TableName: tableName, IsFirst: false}
		select {
		case <-i.cfg.Ctx.Done():
			return i.cfg.Ctx.Err()
		case i.cfg.ChReadSql <- item:
		}
	}
	return nil
}

func (i *importerImplement) execSql(tableName, sqlStr string, isFirst bool) error {
	if !utils.SafeIdent(tableName) {
		return fmt.Errorf("invalid table name %q", tableName)
	}
	queryDB, err := i.queryDB(sqlStr)
	if err != nil {
		return err
	}
	rows, err := i.getRowsFromDb(queryDB, sqlStr)
	if err != nil {
		return fmt.Errorf("error at getRowsFromDb:%v", err)
	}
	defer rows.Close()

	cTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error at get columns types:%v", err)
	}
	if isFirst {
		columnsList := make([]string, len(cTypes))
		for n, col := range cTypes {
			t := col.DatabaseTypeName()
			f := col.Name()
			if !utils.SafeIdent(f) {
				return fmt.Errorf("invalid column name %q", f)
			}
			sqlLiteType, err1 := utils.MapMySQLTypeToSQLite(t)
			if err1 != nil {
				log.Println(err1)
				columnsList[n] = fmt.Sprintf("%s TEXT", f)
			} else {
				nullable, ok := col.Nullable()
				if ok && !nullable {
					sqlLiteType += " NOT NULL"
				}
				if sqlLiteType == "INTEGER" || sqlLiteType == "REAL" {
					sqlLiteType += " DEFAULT 0"
				}
				columnsList[n] = fmt.Sprintf("%s %s", f, sqlLiteType)
			}
		}
		if _, err = i.cfg.DbLocal.ExecContext(i.cfg.Ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			return fmt.Errorf("error dropping SQLite table:%v", err)
		}
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columnsList, ", "))
		if i.cfg.Debug {
			log.Println(createTableSQL)
		}
		if _, err = i.cfg.DbLocal.ExecContext(i.cfg.Ctx, createTableSQL); err != nil {
			return fmt.Errorf("error at creating SQLite table:%v", err)
		}
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting columns: %v", err)
	}
	for _, col := range columns {
		if !utils.SafeIdent(col) {
			return fmt.Errorf("invalid column name %q", col)
		}
	}
	for rows.Next() {
		values := make([]interface{}, len(cTypes))
		valuePtrs := make([]interface{}, len(cTypes))
		for n := range values {
			valuePtrs[n] = &values[n]
		}
		if err = rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}
		rowMap := make(map[string]interface{}, len(columns))
		for p, col := range columns {
			var v interface{}
			val := values[p]
			if b, ok := val.([]byte); ok {
				v = string(b)
			} else {
				v = val
			}
			rowMap[col] = v
		}
		item := &rowBatch{TableName: tableName, Cols: columns, Item: rowMap}
		select {
		case <-i.cfg.Ctx.Done():
			return i.cfg.Ctx.Err()
		case i.cfg.ChWriteRow <- item:
		}
	}
	return rows.Err()
}

// insertBatch 批量插入数据到SQLite
func (i *importerImplement) insertBatch(rows *rowsBatch) error {
	if len(rows.Items) == 0 {
		return nil
	}
	if len(rows.Cols) == 0 {
		return fmt.Errorf("empty column list")
	}
	if !utils.SafeIdent(rows.TableName) {
		return fmt.Errorf("invalid table name %q", rows.TableName)
	}
	for _, col := range rows.Cols {
		if !utils.SafeIdent(col) {
			return fmt.Errorf("invalid column name %q", col)
		}
	}

	columnsList := strings.Join(rows.Cols, ", ")
	placeholdersList := strings.Repeat("?,", len(rows.Cols))
	one := "(" + placeholdersList[:len(placeholdersList)-1] + ")"
	valSql := make([]string, 0, len(rows.Items))
	args := make([]interface{}, 0, len(rows.Items)*len(rows.Cols))
	for _, row := range rows.Items {
		for _, col := range rows.Cols {
			args = append(args, row[col])
		}
		valSql = append(valSql, one)
	}

	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", rows.TableName, columnsList, strings.Join(valSql, ","))
	stmt, err := i.cfg.DbLocal.PrepareContext(i.cfg.Ctx, insertSql)
	if err != nil {
		log.Printf("error preparing statement: %v", err)
		return err
	}
	defer stmt.Close()
	if _, err = stmt.ExecContext(i.cfg.Ctx, args...); err != nil {
		log.Printf("error executing batch insert: %v", err)
		return err
	}
	return nil
}

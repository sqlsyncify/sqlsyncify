package importer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/svc"
	"sqlsyncify/internal/utils"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
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

type Config struct {
	// ChBatch   chan rowsBatch
	ChRow     chan *rowBatch
	Db        *sql.DB
	ExDB      *sql.DB
	DbLocal   *sql.DB
	Site      string
	Ctx       context.Context
	BatchSize int
	BatchCore int
	Debug     bool
	AppConf   config.Config
	SiteConf  *config.SiteConfig
}

type Importer interface {
	Run() error
}

type importerImplement struct {
	cfg *Config
}

func NewImporter(cfg *Config) Importer {
	//载入本地db的批量200-400,不能太大,字段多时insert into会报too many SQL variables
	cfg.BatchSize = 200
	cfg.BatchCore = runtime.NumCPU()
	return &importerImplement{cfg: cfg}
}

func (i *importerImplement) getRowsFromDb(exDB *sql.DB, sql string, args ...any) (*sql.Rows, error) {
	// 以SELECT开头的 && 没有limit && 如果开启了limit -> 附加limit在末尾
	sel := utils.IsPrefix(sql, "SELECT")
	nolimit := !strings.Contains(utils.RemoveSqlComment(sql), "LIMIT ")
	if sel && nolimit && i.cfg.SiteConf.EnabledImportLimit() {
		sql += fmt.Sprintf(" LIMIT %d", i.cfg.SiteConf.ImportLimit)
		// if i.cfg.Debug {
		// 	log.Println("LIMIT ", i.cfg.AppConf.LocalImportLimit)
		// }
	}
	if exDB != nil {
		return exDB.QueryContext(i.cfg.Ctx, sql, args...)
	}
	return i.cfg.Db.QueryContext(i.cfg.Ctx, sql, args...)
}

// Run 导入远程mysql数据
// 写入本地db表
func (i *importerImplement) Run() error {
	dirPath := fmt.Sprintf("./etc/sites/%s/sql-import/", i.cfg.Site)
	// 使用os.Stat获取文件信息
	_, err := os.Stat(dirPath)

	if os.IsNotExist(err) {
		return fmt.Errorf("[Import] site:%s does not found", i.cfg.Site)
	} else if err != nil {
		// 其他错误，例如权限问题
		return err
		// } else {
		//站点目录存在
	}
	sqlFiles, err := utils.ScanDir(dirPath)

	if err != nil {
		log.Printf("error walking the directory %s: %v\n", dirPath, err)
		return err
	}
	// 按文件名排序（字母顺序）
	// sort.Slice(sqlFiles, func(i, j int) bool {
	// 	return sqlFiles[i] < sqlFiles[j]
	// })

	//多表并发写
	i.cfg.ChRow = make(chan *rowBatch, i.cfg.BatchCore)
	var WgWrite sync.WaitGroup
	WgWrite.Add(i.cfg.BatchCore)
	for c := 1; c < i.cfg.BatchCore+1; c++ {
		go func(workerId int) {
			count := 0
			buf := make(map[string]*rowsBatch, i.cfg.BatchSize)
			defer func() {
				WgWrite.Done()
				buf = nil
			}()
			for item := range i.cfg.ChRow {
				count++
				obj, ok := buf[item.TableName]
				if !ok {
					obj = &rowsBatch{TableName: item.TableName, Cols: item.Cols}
					buf[item.TableName] = obj
				}
				obj.Items = append(obj.Items, item.Item)
				if len(obj.Items) == i.cfg.BatchSize {
					if i.cfg.Debug {
						log.Printf("[worker-%03d] %s flush %d, size:%d\n ", workerId, item.TableName, count, len(obj.Items))
					}
					i.insertBatch(obj)
					obj.Items = []map[string]any{}
				}
			}
			for _, item := range buf {
				i.insertBatch(item)
			}
			buf = nil
		}(c)
	}

	for _, file := range sqlFiles {
		err = i.loadDataFromSqlFile(file)
		if err != nil {
			log.Println(file, err)
		}
	}

	// close(i.ChBatch)
	close(i.cfg.ChRow)
	log.Println("load sql files end.")

	log.Println("waiting for all import workers...")
	select {
	case <-i.cfg.Ctx.Done():
		return i.cfg.Ctx.Err()
	default:
		WgWrite.Wait()
		break
	}
	i.sqliteReSize()

	return nil
}

// 经常删除数据,回收sqlite文件占用空间
func (i *importerImplement) sqliteReSize() {
	_, err := i.cfg.DbLocal.Exec("VACUUM;")
	if err != nil {
		log.Println("sqliteConn Exec VACUUM error:", err)
	}
}

// 提交SQL到远程数据源抽取数据
func (i *importerImplement) loadDataFromSqlFile(file string) error {
	var exDb *sql.DB
	defer func() {
		if exDb != nil {
			_ = exDb.Close()
			exDb = nil
		}
	}()
	log.Println("Load File:", file)
	sqlf, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("error at load file:%v", err)
	}
	sqlStr := string(sqlf)
	if strings.Contains(sqlStr, "{lang}") {
		sqlStr = strings.ReplaceAll(sqlStr, "{lang}", i.cfg.SiteConf.Lang)
	}
	if false == utils.IsPrefix(sqlStr, "SELECT") {
		return fmt.Errorf("invalid sql: do not start with SELECT")
	}

	//sql文件名做新表名
	fileNames := strings.Split(file, "/")
	fName := fileNames[len(fileNames)-1]
	tableName := fName[strings.Index(fName, "_")+1 : strings.Index(fName, ".")]
	log.Println("tableName:", tableName)
	if i.cfg.Debug {
		log.Println("sql:", sqlStr)
	}

	//sql文件中指定数据源时，要连接新数据源
	dsPos := strings.Index(sqlStr, "-- ds=")
	if dsPos != -1 {
		end := strings.Index(sqlStr, "\n")
		ds := sqlStr[dsPos+len("-- ds=") : end]
		ds = strings.Trim(ds, " \t\r")
		log.Println("connect external data source=", ds)

		if exDb != nil {
			_ = exDb.Close()
			exDb = nil
		}

		exDb, err = svc.NewDbConn(ds)
		if err != nil {
			return fmt.Errorf("error at connect exDb:%v", err)
		}
	}
	//读取远程数据
	//TODO 远程表数据上千万行时，要拆分数据行，分批读取
	rows, err := i.getRowsFromDb(exDb, sqlStr)
	if err != nil {
		return fmt.Errorf("error at getRowsFromDb:%v", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	//空行时要建空表, 避免导出查询报错

	// 创建新表
	// 构建CREATE TABLE语句
	//获取每列的数据类型
	cTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error at get columns types:%v", err)
	}
	columnsList := make([]string, len(cTypes))
	columns, _ := rows.Columns()
	for i, col := range cTypes {
		t := col.DatabaseTypeName()
		f := col.Name()
		sqlLiteType, err1 := utils.MapMySQLTypeToSQLite(t)
		if err1 != nil {
			log.Println(err1)
			columnsList[i] = fmt.Sprintf("%s TEXT", f)
		} else {
			// 处理是否可为空
			nullable, ok := col.Nullable()
			if ok && !nullable {
				sqlLiteType += " NOT NULL"
			}
			if sqlLiteType == "INTEGER" || sqlLiteType == "REAL" {
				sqlLiteType += " DEFAULT 0"
			}
			columnsList[i] = fmt.Sprintf("%s %s", f, sqlLiteType)
		}
	}
	_, _ = i.cfg.DbLocal.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columnsList, ", "))
	// 执行CREATE TABLE语句
	if i.cfg.Debug {
		log.Println(createTableSQL)
	}
	_, err = i.cfg.DbLocal.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error at creating SQLite table:%v", err)
	}

	// 读取MySQL数据并写入SQLite
	for rows.Next() {
		// 创建一个切片来存储每个字段的地址
		values := make([]interface{}, len(cTypes))
		valuePtrs := make([]interface{}, len(cTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描每一行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// 将结果存储到map中
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

		//多表并发写
		i.cfg.ChRow <- &rowBatch{TableName: tableName, Cols: columns, Item: rowMap}

	}
	return nil
}

// insertBatch 批量插入数据到SQLite
func (i *importerImplement) insertBatch(rows *rowsBatch) {
	if len(rows.Items) == 0 {
		log.Println("empty data")
		return
	}

	// 构建INSERT语句
	columnsList := strings.Join(rows.Cols, ", ")
	placeholdersList := strings.Repeat("?,", len(rows.Cols))
	var valSql []string

	// 准备参数
	args := make([]interface{}, 0, len(rows.Items))
	for _, row := range rows.Items {
		for _, col := range rows.Cols {
			args = append(args, row[col])
		}
		valSql = append(valSql, fmt.Sprintf("(%s)", placeholdersList[:len(placeholdersList)-1]))
	}

	// 执行批量插入
	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", rows.TableName, columnsList, strings.Join(valSql, ","))
	stmt, err := i.cfg.DbLocal.Prepare(insertSql)
	if err != nil {
		log.Printf("error preparing statement: %v, SQL:%s", err, insertSql)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec(args...)
	if err != nil {
		log.Printf("error executing batch insert: %v", err)
	}
}

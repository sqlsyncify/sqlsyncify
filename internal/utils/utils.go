package utils

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"
)

func IsPrefix(str string, prefix string) bool {
	str = RemoveSqlComment(str)
	tmp := str[:len(prefix)]
	return strings.EqualFold(tmp, prefix)
}

func ScanDir(dirPath string) ([]string, error) {
	// 使用filepath.Walk遍历目录
	log.Printf("scan dir: %s\n", dirPath)
	var sqlFiles []string
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 如果在访问路径时发生错误，打印错误并继续
			log.Printf("Error accessing path %s: %v\n", path, err)
			return nil // 继续遍历
		}

		// 检查是否为文件（排除目录）
		if !info.IsDir() && filepath.Ext(path) == ".sql" {
			log.Println("Found File:", path)
			sqlFiles = append(sqlFiles, path)
		}

		return nil // 返回nil以继续遍历
	})
	return sqlFiles, err
}

// GenerateIndexName 生成索引名
func GenerateIndexName(prefix string, tz string) string {
	now := time.Now()
	if len(tz) > 0 {
		loc, err := time.LoadLocation(tz)
		if err == nil {
			now = now.In(loc)
		}
	}
	indexName := now.Format("20060102150405")
	return fmt.Sprintf("%s_%s", prefix, indexName)
}

func ParseSql(sqlstr string) (sqlparser.Statement, error) {
	// Parser with default options. New() itself initializes with default MySQL version.
	parser, err := sqlparser.New(sqlparser.Options{
		TruncateUILen:  512,
		TruncateErrLen: 0,
	})
	if err != nil {
		return nil, err
	}
	stmt, err := parser.Parse(sqlstr)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func GetFieldsFromSql(sql string) ([]string, error) {
	stmt, err := ParseSql(sql)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)

	// 如果是 SELECT 语句且包含 WITH，清空它
	if ok {
		var columns []string
		for _, expr := range sel.SelectExprs.Exprs {
			// sqlparser.String 会将表达式转回 SQL 字符串
			columns = append(columns, sqlparser.String(expr))
		}
		// xxx as a => a
		for i, field := range columns {
			tmp := strings.ToLower(field)
			for {
				if strings.Contains(tmp, " as ") {
					tmp = tmp[strings.Index(tmp, " as ")+4:]
				} else if strings.Contains(tmp, " ") {
					tmp1 := strings.Split(tmp, " ")
					tmp = tmp1[len(tmp1)-1]
				} else {
					break
				}
			}
			columns[i] = tmp
		}
		return columns, nil
	}
	return nil, errors.New("not a select statement")
}

// RemoveSqlBracket 清除SQL括号
func RemoveSqlBracket(str string) string {
	str = RemoveSqlComment(str)
	var s, e int
	for {
		// 移除括号
		if strings.Contains(str, "(") && strings.Contains(str, ")") {
			s = strings.Index(str, "(")
			e = strings.LastIndex(str, ")")
			str = strings.Replace(str, str[s:e+1], "", 1)
		} else {
			break
		}
	}
	return str
}

// RemoveWithClause 清除WITH查询
func RemoveWithClause(sql string) (string, error) {
	// Parser with default options. New() itself initializes with default MySQL version.
	parser, err := sqlparser.New(sqlparser.Options{
		TruncateUILen:  512,
		TruncateErrLen: 0,
	})
	if err != nil {
		return "", err
	}
	stmt, err := parser.Parse(sql)
	if err != nil {
		return "", err
	}

	// 如果是 SELECT 语句且包含 WITH，清空它
	if sel, ok := stmt.(*sqlparser.Select); ok {
		sel.With = nil // 直接移除 WITH 子句
	}

	return sqlparser.String(stmt), nil
}

// RemoveSqlComment 清除SQL注释行
func RemoveSqlComment(str string) string {
	var s, e int
	for {
		str = strings.TrimSpace(str)
		if strings.ToUpper(str[:4]) == "WITH" {
			str1, err := RemoveWithClause(str)
			if err != nil {
				str = err.Error()
			} else {
				str = str1
			}
		} else if str[:3] == "-- " {
			s = strings.Index(str, "\n")
			str = str[s+1:]
		} else if strings.Contains(str, "/*") && strings.Contains(str, "*/") {
			// 移除注释
			s = strings.Index(str, "/*")
			e = strings.Index(str, "*/")
			str = strings.ReplaceAll(str, str[s:e+2], "")
		} else {
			break
		}
	}
	return str
}

// MapMySQLTypeToSQLite 映射 MySQL 类型到 SQLite 类型
func MapMySQLTypeToSQLite(mysqlType string) (string, error) {
	t := strings.ToUpper(mysqlType)
	t = strings.ReplaceAll(t, "UNSIGNED ", "")
	switch t {
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return "INTEGER", nil
	case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return "TEXT", nil
	case "DATE", "DATETIME", "TIMESTAMP":
		return "TEXT", nil // SQLite 没有专门的 DATE 类型
	case "FLOAT", "DOUBLE", "DECIMAL", "REAL":
		return "REAL", nil
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return "BLOB", nil
	default:
		return "", fmt.Errorf("未知的 MySQL 类型: %s", mysqlType)
	}
}

func CompareVersion(a, b string) (int, error) {
	if a == "" || b == "" {
		return 0, errors.New("require 2 version strings")
	}
	// a大于b返回1，a等于b返回0，a小于b返回-1
	va := strings.Split(a, ".")
	vb := strings.Split(b, ".")
	bl := len(vb)
	al := len(va)
	for i := 0; i < max(al, bl); i++ {
		if bl-1 < i {
			return 1, nil
		} else if al-1 < i {
			return -1, nil
		}
		aa, err := strconv.Atoi(va[i])
		if err != nil {
			return 0, err
		}
		bb, err := strconv.Atoi(vb[i])
		if err != nil {
			return 0, err
		}
		if aa > bb {
			return 1, nil
		} else if aa < bb {
			return -1, nil
		}
	}
	return 0, nil
}

func InArray[T comparable](slice []T, item T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func RemoveElement[T comparable](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}

func CheckSiteFormat(input string) bool {
	// 允许：字母、数字、下划线、连字符
	pattern := `^[a-zA-Z0-9_\-@]+$`

	matched, _ := regexp.MatchString(pattern, input)
	return matched
}

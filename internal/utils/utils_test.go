package utils

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestGenerateIndexName(t *testing.T) {
	tmp := GenerateIndexName("test", "")
	fmt.Println(tmp)
	if !strings.HasPrefix(tmp, "test") {
		t.Errorf("index name error")
	}
}

func TestPrefix(t *testing.T) {
	tmp := `
	/*
	abc
	123
	*/
	-- abc
	SELECT 1
	`
	a := IsPrefix(tmp, "SELECT")
	if !a {
		t.Fatal("FAIL")
	}
	t.Log(os.Getwd())
	sql, err := os.ReadFile("../../etc/wordpress/sql-import/posts.sql")
	if err != nil {
		t.Fatal(err)
	}
	b := IsPrefix(string(sql), "SELECT")
	if !b {
		t.Fatal("FAIL")
	}
	t.Log("OK")
}

func TestRemoveSqlBracket(t *testing.T) {
	sql := `select a, b from table_test`
	txt := RemoveSqlBracket(sql)
	if strings.Contains(txt, "(") || strings.Contains(txt, ")") {
		t.Fatal(txt)
	}
	fmt.Println(txt)
}

func TestGetFieldsFromSql(t *testing.T) {
	sql := `select a, b from table_test `
	fields, err := GetFieldsFromSql(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("empty")
	}
	fmt.Println(fields)
}

func TestGetFieldsFromSql1(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name string
		arg  args
		want []string
	}{
		{name: "t1", arg: args{sql: "select a,b from table_test"}, want: []string{"a", "b"}},
		{name: "t3", arg: args{sql: "select a as aa,b as bb from table_test"}, want: []string{"aa", "bb"}},
		{name: "t2", arg: args{sql: "select * from table_test"}, want: []string{"*"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetFieldsFromSql(tt.arg.sql)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("got", got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFieldsFromSql(%s) = %v, want %v", tt.arg.sql, got, tt.want)
			}
		})
	}
}

func TestCompareVersion(t *testing.T) {
	type args struct {
		a string
		b string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{name: "t1", args: args{a: "1.1", b: "1.1"}, want: 0},
		{name: "t2", args: args{a: "1.2", b: "1.1"}, want: 1},
		{name: "t3", args: args{a: "1.1", b: "1.0"}, want: 1},
		{name: "t4", args: args{a: "1.1", b: "1.2"}, want: -1},
		{name: "t5", args: args{a: "1.2", b: "1.0"}, want: 1},
		{name: "t6", args: args{a: "1.10.0", b: "1.9.1"}, want: 1},
		{name: "t7", args: args{a: "1.9.0", b: "1.90.1"}, want: -1},
		{name: "t8", args: args{a: "0.1.0", b: ""}, want: 0, wantErr: true},
		{name: "t9", args: args{a: "", b: ""}, want: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareVersion(tt.args.a, tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CompareVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInArray(t *testing.T) {
	type args[T comparable] struct {
		slice []T
		item  T
	}
	type testCase[T comparable] struct {
		name string
		args args[T]
		want bool
	}
	tests := []testCase[string]{
		{name: "t1", args: args[string]{slice: []string{"a", "b"}, item: "a"}, want: true},
		{name: "t1", args: args[string]{slice: []string{"a", "b"}, item: "b"}, want: true},
		{name: "t1", args: args[string]{slice: []string{"a", "b"}, item: "c"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InArray(tt.args.slice, tt.args.item); got != tt.want {
				t.Errorf("InArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSimplePathCheck(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "a1", args: args{"abc"}, want: true},
		{name: "a2", args: args{"-123"}, want: true},
		{name: "a3", args: args{"/*-"}, want: false},
		{name: "a4", args: args{".a123"}, want: false},
		{name: "a5", args: args{"+987"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckSiteFormat(tt.args.input); got != tt.want {
				t.Errorf("CheckSiteFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

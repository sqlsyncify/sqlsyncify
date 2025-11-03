package export

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sqlsyncify/internal/config"
	"sqlsyncify/internal/utils"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ExporterConfig struct {
	AppConf       config.Config
	DbLocal       *sql.DB
	SiteConf      *config.SiteConfig
	Ctx           context.Context
	EsClient      *elasticsearch.Client
	FullIndexName string
	Debug         bool
	DocIdKey      string
}

type Exporter interface {
	Run() (uint64, error)
	Alias() error
}

type exporterImplement struct {
	cfg             *ExporterConfig
	cfgv5           *ExporterConfigV5
	countSuccessful uint64
	countFail       uint64
}

// NewExporter 入口
func NewExporter(config *ExporterConfig) Exporter {
	if len(config.FullIndexName) == 0 {
		// the new index name
		config.FullIndexName = utils.GenerateIndexName(config.SiteConf.IndexName, config.SiteConf.TimeZone)
	}
	return &exporterImplement{cfg: config}
}

// v8 es client
func (exp *exporterImplement) initClient() error {
	log.Println("conf.EsCluster", exp.cfg.SiteConf.EsCluster)
	if len(exp.cfg.SiteConf.EsCluster) == 0 {
		return errors.New("require es cluster addr")
	}
	// https://github.com/elastic/go-elasticsearch/blob/main/_examples/bulk/indexer.go
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(exp.cfg.SiteConf.EsCluster, ","),
		// APIKey:    conf.EsApiKey,
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},
		// Retry up to 5 attempts
		//
		MaxRetries: 3,
		// EnableDebugLogger: true,
	})
	if err != nil {
		return errors.New(fmt.Sprintf("NewExporter, create es client error:%s", err.Error()))
	}
	exp.cfg.EsClient = esClient
	return nil
}

// Run 导出到es
func (exp *exporterImplement) Run() (uint64, error) {
	v, _ := utils.CompareVersion(exp.cfg.SiteConf.EsVersion, "6.0")
	if v == -1 {
		return exp.runV5()
	}
	err := exp.initClient()
	if err != nil {
		return 0, err
	}
	log.Println("conf.EsCluster", exp.cfg.SiteConf.EsCluster)
	if len(exp.cfg.SiteConf.EsCluster) == 0 {
		return 0, errors.New("ExportEs, cannot empty es cluster addr")
	}
	dirPath := fmt.Sprintf("./etc/sites/%s/sql-export/", exp.cfg.SiteConf.Site)
	// 使用os.Stat获取文件信息
	_, err = os.Stat(dirPath)
	if os.IsNotExist(err) {
		//不存在站点目录
		return 0, fmt.Errorf("[Export] site:%s do not found sql-export path", exp.cfg.SiteConf.Site)
	} else if err != nil {
		// 其他错误，例如权限问题
		return 0, err
	}
	sqlFiles, _ := utils.ScanDir(dirPath)

	mapping, err := os.ReadFile(fmt.Sprintf("etc/sites/%s/mapping.json", exp.cfg.SiteConf.Site))
	if err != nil {
		return 0, errors.New(fmt.Sprintf("ExportEs, read mapping error: %s", err.Error()))
	}
	setting, err := os.ReadFile(fmt.Sprintf("etc/sites/%s/setting.json", exp.cfg.SiteConf.Site))
	if err != nil {
		return 0, errors.New(fmt.Sprintf("ExportEs, read setting error: %s", err.Error()))
	}

	// setting替换关键词
	setting = exp.filterSetting(setting)
	mapping = exp.filterSetting(mapping)

	log.Println("ready to create new index:", exp.cfg.FullIndexName)
	body := fmt.Sprintf(`{
		  "settings": %s,
		  "mappings": %s
		}`, setting, mapping)
	if exp.cfg.Debug {
		log.Println(body)
	}

	res, err := exp.cfg.EsClient.Indices.Create(exp.cfg.FullIndexName,
		exp.cfg.EsClient.Indices.Create.WithBody(strings.NewReader(body)),
	)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("ExportEs, cannot create es index: %s", err.Error()))
	}
	if res.IsError() {
		return 0, errors.New(fmt.Sprintf("ExportEs, cannot create es index: %s", res.String()))
	}
	_ = res.Body.Close()

	// Create the BulkIndexer
	//
	// NOTE: For optimal performance, consider using a third-party JSON decoding package.
	//       See an example in the "benchmarks" folder.
	//
	bulkCfg := esutil.BulkIndexerConfig{
		Index:  exp.cfg.FullIndexName, // The default index name
		Client: exp.cfg.EsClient,      // The Elasticsearch client
		// DebugLogger: log.New(os.Stdout, "esBulk", 0),
	}
	// if exp.cfg.Debug {
	// 	bulkCfg.DebugLogger = log.New(os.Stdout, "esBulk", 0)
	// }
	bulkIndexer, err := esutil.NewBulkIndexer(bulkCfg)
	if err != nil {
		return 0, errors.New("ExportEs, creating the indexer:" + err.Error())
	}

	start := time.Now().UTC()
	for _, file := range sqlFiles {
		err = exp.loadDataFromSqlFile(file, bulkIndexer)
		if err != nil {
			log.Println(file, err)
		}
	}

	log.Println("waiting for all workers...")
	// waiting and close the indexer
	if err := bulkIndexer.Close(exp.cfg.Ctx); err != nil {
		log.Printf("Unexpected error: %s", err)
		return 0, err
	}
	//success & fail report
	biStats := bulkIndexer.Stats()
	dur := time.Since(start)
	if exp.cfg.Debug {
		if biStats.NumFailed > 0 {
			log.Printf(
				"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)\n",
				humanize.Comma(int64(biStats.NumFlushed)),
				humanize.Comma(int64(biStats.NumFailed)),
				dur.Truncate(time.Millisecond),
				humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
		} else {
			log.Printf(
				"Sucessfuly indexed [%s] documents in %s (%s docs/sec)\n",
				humanize.Comma(int64(biStats.NumFlushed)),
				dur.Truncate(time.Millisecond),
				humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
		}
	}
	log.Println("ExportEs done, numErrors:", biStats.NumFailed, ", ", "numSuccess:", biStats.NumFlushed)
	if biStats.NumFlushed+biStats.NumFailed == 0 {
		return 0, nil
	}
	percent := uint64((float32(biStats.NumFlushed) / float32(biStats.NumFlushed+biStats.NumFailed)) * 100)
	return percent, nil
}

// 装载数据
func (exp *exporterImplement) loadDataFromSqlFile(file string, bulkIndexer esutil.BulkIndexer) error {
	log.Println("Load File:", file)
	sqlf, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	sqlStr := string(sqlf)
	// 不以SELECT开头的,就不用处理查询结果
	// 原则上一个站点一次只做写入一个索引, 但是可以做SQL分页查询导出到同一个索引
	if false == utils.IsPrefix(sqlStr, "SELECT") {
		_, err = exp.cfg.DbLocal.Exec(sqlStr)
		if err != nil {
			log.Fatalln(file, " error:", err)
		}
		return nil
	}
	rows, err := exp.cfg.DbLocal.QueryContext(exp.cfg.Ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("error euery: %v", err)
	}

	defer func() {
		_ = rows.Close()
	}()

	// 获取字段列表
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting columns: %v", err)
	}

	var count = 0
	for rows.Next() {
		// 创建一个切片来存储每个字段的地址
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描每一行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning row: %v\n", err)
			continue
		}
		// 将结果存储到map中
		result := make(map[string]any)
		for i, col := range columns {
			var v any
			val := values[i]
			if b, ok := val.([]byte); ok {
				v = string(b)
			} else {
				v = val
			}
			result[col] = v
		}
		primaryKey := exp.cfg.SiteConf.DocIdKey

		exp.formatFields(result)

		// Prepare the data payload: encode article to JSON
		//
		jsonBody, err := json.Marshal(result)
		if err != nil {
			log.Printf("Cannot encode sku %s: %s \n", result[primaryKey], err)
			continue
		}
		if count < 1 {
			log.Println(string(jsonBody))
		}
		count++

		// Add an item to the BulkIndexer
		//
		docId := fmt.Sprintf("%v", result[primaryKey])
		err = bulkIndexer.Add(
			exp.cfg.Ctx,
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",
				// DocumentID is the (optional) document ID
				DocumentID: docId,
				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(jsonBody),
				// OnSuccess is called for each successful operation
				OnSuccess: exp.bulkOnSuccess,
				// OnFailure is called for each failed operation
				OnFailure: exp.bulkOnFailure,
			},
		)
		if err != nil {
			log.Printf("Unexpected error(bulkIndexer.Add): %s \n", err)
		}
	}
	return nil
}

func (exp *exporterImplement) bulkOnFailure(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	if err != nil {
		log.Println("ERROR: ", err, " DocumentID: ", item.DocumentID)
	} else {
		log.Println("ERROR: ", res.Error.Type, res.Error.Reason, " DocumentID: ", item.DocumentID)
	}
	atomic.AddUint64(&exp.countFail, 1)
}

func (exp *exporterImplement) bulkOnSuccess(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
	// log.Println("res", res)
	atomic.AddUint64(&exp.countSuccessful, 1)
}

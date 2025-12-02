package export

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sqlsyncify/internal/utils"
	"strings"
	"sync"
	"sync/atomic"

	elasticsearchV5 "github.com/elastic/go-elasticsearch/v5"
	esapiV5 "github.com/elastic/go-elasticsearch/v5/esapi"
)

type ExporterConfigV5 struct {
	EsClientV5 *elasticsearchV5.Client
	ChBatch    chan *bulkIndexerItemV5
	BatchCores int
	BatchNum   int
	// 5M
	BatchSizeBytes int
	WgWriteEs      sync.WaitGroup
}

type bulkIndexerItemV5 struct {
	Meta       []byte
	Body       []byte
	DocumentID string
	docType    string
}

type bulkResponseV5 struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

func (exp *exporterImplement) initV5() error {
	exp.cfgv5 = &ExporterConfigV5{
		BatchCores: runtime.GOMAXPROCS(runtime.NumCPU()),
		// 5M
		BatchSizeBytes: 5 * 1000 * 1000,
		BatchNum:       200,
	}

	// https://github.com/elastic/go-elasticsearch/blob/main/_examples/bulk/indexer.go
	esClientV5, err := elasticsearchV5.NewClient(elasticsearchV5.Config{
		Addresses: strings.Split(exp.cfg.SiteConf.EsCluster, ","),
	})
	if err != nil {
		return errors.New("ExportEs, create es client error:" + err.Error())
	}
	exp.cfgv5.EsClientV5 = esClientV5
	return nil
}

// runV5 导出到es
// 返回成功率百分比
func (exp *exporterImplement) runV5() (uint64, error) {
	log.Println("conf.EsClusterV5", exp.cfg.SiteConf.EsCluster)
	if len(exp.cfg.SiteConf.EsCluster) == 0 {
		return 0, errors.New("ExportV5, require es cluster v5 addr")
	}
	if exp.cfg.Debug {
		log.Println("run v5, es v5 addr:", exp.cfg.SiteConf.EsCluster)
	}

	err := exp.initV5()
	if err != nil {
		return 0, err
	}

	dirPath := fmt.Sprintf("etc/sites/%s/sql-export/", exp.cfg.SiteConf.Site)
	// 使用os.Stat获取文件信息
	_, err = os.Stat(dirPath)
	if os.IsNotExist(err) {
		// 目录不存在
		return 0, fmt.Errorf("[export v5] site:%s does not found", exp.cfg.SiteConf.Site)
	} else if err != nil {
		// 其他错误，例如权限问题
		return 0, err
	}
	sqlFiles, _ := utils.ScanDir(dirPath)

	mapping, err := os.ReadFile(fmt.Sprintf("etc/sites/%s/mapping_v5.json", exp.cfg.SiteConf.Site))
	if err != nil {
		return 0, errors.New("ExportEs, read mapping error:" + err.Error())
	}
	setting, err := os.ReadFile(fmt.Sprintf("etc/sites/%s/setting_v5.json", exp.cfg.SiteConf.Site))
	if err != nil {
		return 0, errors.New("ExportEs, read setting error:" + err.Error())
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

	res, err := exp.cfgv5.EsClientV5.Indices.Create(exp.cfg.FullIndexName,
		exp.cfgv5.EsClientV5.Indices.Create.WithBody(strings.NewReader(body)),
	)
	if err != nil {
		return 0, errors.New("ExportEs, cannot create es index:" + err.Error())
	} else if res.IsError() {
		return 0, errors.New("ExportEs, cannot create es index:" + res.String())
	}
	_ = res.Body.Close()

	//准备用于接收写入数据的队列
	log.Println("batchCores:", exp.cfgv5.BatchCores)
	exp.cfgv5.ChBatch = make(chan *bulkIndexerItemV5, exp.cfgv5.BatchCores)
	var numSuccess, numErrors uint64

	exp.cfgv5.WgWriteEs.Add(exp.cfgv5.BatchCores)
	for c := 1; c < exp.cfgv5.BatchCores+1; c++ {
		go func(workerId int) {
			count := 0
			var buf bytes.Buffer
			docType := ""
			defer func() {
				exp.cfgv5.WgWriteEs.Done()
			}()
			for item := range exp.cfgv5.ChBatch {
				if _, err = buf.Write(item.Meta); err != nil {
					log.Printf("[worker-%03d] Read Meta [%s] error:%s", workerId, item.DocumentID, err)
					continue
				}
				if _, err = buf.Write(item.Body); err != nil {
					log.Printf("[worker-%03d] Read Body [%s] error:%s", workerId, item.DocumentID, err)
					continue
				}
				docType = item.docType
				count++
				if buf.Len() >= exp.cfgv5.BatchSizeBytes || count%exp.cfgv5.BatchNum == 0 {
					// log.Printf("[worker-%03d] flush %d, size:%d\n ", workerId, count, buf.Len())
					succ, errs := exp.insertBatch(buf.Bytes(), docType)
					buf.Reset()
					atomic.AddUint64(&numSuccess, succ)
					atomic.AddUint64(&numErrors, errs)
				}
			}
			if buf.Len() > 0 {
				log.Printf("[worker-%03d] flush %d, size:%d\n ", workerId, count, buf.Len())
				succ, errs := exp.insertBatch(buf.Bytes(), docType)
				atomic.AddUint64(&numSuccess, succ)
				atomic.AddUint64(&numErrors, errs)
			}
			buf.Reset()
		}(c)
	}

	for _, file := range sqlFiles {
		err = exp.loadDataFromSqlFileV5(file)
		if err != nil {
			log.Println(file, err)
		}
	}
	// 读取完要关闭通道
	close(exp.cfgv5.ChBatch)

	log.Println("waiting for all workers...")

	select {
	case <-exp.cfg.Ctx.Done():
		return 0, exp.cfg.Ctx.Err()
	default:
		exp.cfgv5.WgWriteEs.Wait()
	}
	log.Println("ExportEsV5 done, numSuccess:", numSuccess, "numErrors", numErrors)
	if numSuccess+numErrors == 0 {
		return 0, nil
	}
	percent := uint64((float32(numSuccess) / float32(numSuccess+numErrors)) * 100)
	return percent, nil
}

// 批量写入本地文件,用于调试
func (exp *exporterImplement) insertBatchLocalFile(buf []byte) {
	// 打开文件（如果文件不存在则创建）,追加模式
	file, err := os.OpenFile(fmt.Sprintf("storage/error_%s", exp.cfg.FullIndexName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("error:打开文件时出错:", err)
		return
	}
	defer func() {
		_ = file.Close()
	}()
	_, err = file.Write(buf)
	if err != nil {
		log.Println("error:写入文件时出错:", err)
		return
	}
}

// 批量写入es
func (exp *exporterImplement) insertBatch(buf []byte, docType string) (uint64, uint64) {

	req := esapiV5.BulkRequest{Index: exp.cfg.FullIndexName, DocumentType: docType, Body: bytes.NewReader(buf)}
	res, err := req.Do(exp.cfg.Ctx, exp.cfgv5.EsClientV5)

	if err != nil {
		log.Printf("error: Failure indexing batch: %s\n", err)
		return 0, 0
	}
	// If the whole request failed, print error and mark all documents as failed
	//
	var (
		raw        map[string]interface{}
		blk        *bulkResponseV5
		numErrors  uint64
		numSuccess uint64
	)
	if res.IsError() {
		log.Println("error post:", string(buf))
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Printf("error: Failure to to parse response body: %s \n", err)
		} else {
			// log
			exp.insertBatchLocalFile(buf)

			log.Printf("error: [%d] %s: %s",
				res.StatusCode,
				raw["error"].(map[string]interface{})["type"],
				raw["error"].(map[string]interface{})["reason"],
			)
		}
		// A successful response might still contain errors for particular documents...
		//
	} else {
		if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
			log.Printf("error: Failure to to parse response body: %s\n", err)
		} else {
			for _, d := range blk.Items {
				// ... so for any HTTP status above 201 ...
				//
				if d.Index.Status > 201 {
					// ... increment the error counter ...
					//
					numErrors++

					// ... and print the response status and error information ...
					log.Printf("error: [%d]: %s: %s: %s: %s \n",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				} else {
					numSuccess++
				}
			}
		}
	}

	return numSuccess, numErrors
}

// 读取数据
func (exp *exporterImplement) loadDataFromSqlFileV5(file string) error {
	log.Println("Load File:", file)
	// es5.6 doc type
	docType := exp.cfg.SiteConf.DocTypeName
	// es 5.6 doc type 不能以下滑线开头
	docType = strings.TrimPrefix(docType, "_")

	sqlf, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("readFile error:%v", err)
	}
	sqlStr := string(sqlf)
	log.Println(sqlStr)
	// 不以SELECT开头的,就不用处理查询结果
	if sel := utils.IsPrefix(sqlStr, "SELECT"); !sel {
		_, err = exp.cfg.DbLocal.ExecContext(exp.cfg.Ctx, sqlStr)
		if err != nil {
			return fmt.Errorf("query error:%v", err)
		}
		return nil
	}
	rows, err := exp.cfg.DbLocal.QueryContext(exp.cfg.Ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("query error:%v", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	// 获取字段列表
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error at get columns:%v", err)
	}

	for rows.Next() {
		// 创建一个切片来存储每个字段的地址
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描每一行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Println(file, "error scanning row:", err)
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
			log.Printf("error: fail at encode sku %v: %v \n", result[primaryKey], err)
			continue
		}
		docId := fmt.Sprintf("%v", result[primaryKey])
		// Prepare the metadata payload
		//
		meta := []byte(fmt.Sprintf(`{"index":{"_id":"%s","_type":"%s"}}%s`, docId, docType, "\n"))

		// Append newline to the data payload
		//
		jsonBody = append(jsonBody, "\n"...) // <-- Comment out to trigger failure for batch
		// 在协程中再去积攒批量
		exp.cfgv5.ChBatch <- &bulkIndexerItemV5{DocumentID: docId, docType: docType, Body: jsonBody, Meta: meta}

	}
	return nil
}

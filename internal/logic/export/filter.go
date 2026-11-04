package export

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/leeqvip/gophp"
	"sqlsyncify/internal/utils"
)

var metaKeyAliases = map[string]string{
	"_wp_attachment_metadata": "wp_attachment_meta_data",
	"_product_attributes":     "product_attributes",
}

// setting替换关键词
func (exp *exporterImplement) filterSetting(setting []byte) ([]byte, error) {
	if !utils.CheckSiteFormat(exp.cfg.SiteConf.Site) {
		return nil, errors.New("invalid site")
	}
	lang := exp.cfg.SiteConf.Lang
	if lang != "" && !utils.CheckLangFormat(lang) {
		return nil, errors.New("invalid lang")
	}
	host := ""
	if bytes.Contains(setting, []byte("{host}")) {
		var err error
		host, err = utils.SafeHTTPBase(exp.cfg.AppConf.AppHost)
		if err != nil {
			return nil, err
		}
	}
	out := strings.ReplaceAll(string(setting), "{host}", host)
	out = strings.ReplaceAll(out, "{site}", exp.cfg.SiteConf.Site)
	out = strings.ReplaceAll(out, "{lang}", lang)
	return []byte(out), nil
}

// 处理json字段
func (exp *exporterImplement) formatFields(result map[string]any) {

	fieldList := []string{
		"categories", "metaArrayJson",
	}
	exp.formatJsonFields(fieldList, result)

	metaJson, ok := result["metaArrayJson"]
	if !ok {
		return
	}

	seriaKeys := []string{"_wp_attachment_metadata", "_product_attributes"}
	meta, ok2 := metaJson.([]interface{})
	if !ok2 {
		return
	}
	exp.phpUnSerialize(seriaKeys, meta, result)

}

func (exp *exporterImplement) metaMapKeys(k string) string {
	if ret, ok := metaKeyAliases[k]; ok {
		return ret
	}
	return k
}

func (exp *exporterImplement) phpUnSerialize(keys []string, metaJson []interface{}, result map[string]any) {
	kept := make([]interface{}, 0, len(metaJson))
	for _, m := range metaJson {
		metaMaps, ok := m.(map[string]interface{})
		if !ok {
			kept = append(kept, m)
			continue
		}
		matched := false
		for k, val := range metaMaps {
			if !utils.InArray(keys, k) {
				continue
			}
			matched = true
			vv, isString := val.(string)
			if !isString || !utils.PHPSerializeBounded(vv) {
				log.Println("phpUnserialize rejected unsafe payload for", k)
				result[exp.metaMapKeys(k)] = nil
				continue
			}
			tmpv, err := safeUnserialize(vv)
			if err == nil {
				result[exp.metaMapKeys(k)] = tmpv
			} else {
				log.Println("phpUnserialize fail:", err)
				result[exp.metaMapKeys(k)] = nil
			}
		}
		if !matched {
			kept = append(kept, m)
		}
	}
	result["metaArrayJson"] = kept
}

func safeUnserialize(vv string) (value any, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New("php unserialize panicked")
		}
	}()
	return gophp.Unserialize([]byte(vv))
}

// 需要json解码的字段处理
func (exp *exporterImplement) formatJsonFields(fields []string, result map[string]any) {
	var err error
	for _, field := range fields {
		var tmpObj []interface{}
		tmp, ok5 := result[field].(string)
		if ok5 && len(tmp) > 0 {
			err = json.Unmarshal([]byte(tmp), &tmpObj)
			if err != nil {
				log.Printf("json decode field: %s, error:%v\n%v", field, err, tmp)
				continue
			}
			result[field] = tmpObj
		} else {
			result[field] = nil
		}
	}
}

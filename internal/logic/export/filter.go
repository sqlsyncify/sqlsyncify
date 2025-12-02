package export

import (
	"bytes"
	"encoding/json"
	"github.com/leeqvip/gophp"
	"log"
	"sqlsyncify/internal/utils"
)

// setting替换关键词
func (exp *exporterImplement) filterSetting(setting []byte) []byte {
	setting = bytes.ReplaceAll(setting, []byte("{host}"), []byte(exp.cfg.AppConf.AppHost))
	setting = bytes.ReplaceAll(setting, []byte("{site}"), []byte(exp.cfg.SiteConf.Site))
	setting = bytes.ReplaceAll(setting, []byte("{lang}"), []byte(exp.cfg.SiteConf.Lang))
	return setting
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
	internalMapKeys := make(map[string]string)
	internalMapKeys["_wp_attachment_metadata"] = "wp_attachment_meta_data"
	internalMapKeys["_product_attributes"] = "product_attributes"
	if ret, ok := internalMapKeys[k]; ok {
		return ret
	}
	return k
}

func (exp *exporterImplement) phpUnSerialize(keys []string, metaJson []interface{}, result map[string]any) {
	for i, m := range metaJson {
		metaMaps := m.(map[string]interface{})
		for k, val := range metaMaps {
			if utils.InArray(keys, k) == false {
				continue
			}
			vv, ok := val.(string)
			if ok {
				tmpv, err := gophp.Unserialize([]byte(vv))
				if err == nil {
					result[exp.metaMapKeys(k)] = tmpv
				} else {
					log.Println("phpUnserialize fail:", err, vv)
					result[exp.metaMapKeys(k)] = nil
				}
			}
			metaJson = utils.RemoveElement(metaJson, i)
		}
		//if k, ok1 := meta["meta_key"]; ok1 {
		//	if k == nil || utils.InArray(keys, k.(string)) == false {
		//		continue
		//	}
		//	if v, ok2 := meta["meta_value"]; ok2 != false {
		//		tmpv, err := gophp.Unserialize([]byte(v.(string)))
		//		if err == nil {
		//			result[exp.metaMapKeys(k.(string))] = tmpv
		//		} else {
		//			log.Println("phpUnserialize fail:", err, v)
		//			result[exp.metaMapKeys(k.(string))] = nil
		//		}
		//	}
		//	metaJson = utils.RemoveElement(metaJson, i)
		//}
	}
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

package export

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v5/esapi"
)

type IndexAliases struct {
	Aliases map[string]any `json:"aliases"`
}
type MapResponse map[string]IndexAliases

type AliasOperation struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

type UpdateAliasRequest struct {
	Actions []map[string]*UpdateAliasAction `json:"actions"`
}

// UpdateAliasAction represents an action in the Elasticsearch Aliases API.
type UpdateAliasAction struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

func (exp *exporterImplement) aliasV5() error {
	//check exists
	if exp.cfgv5.EsClientV5 == nil {
		return errors.New("require es client v5")
	}
	req1 := esapi.IndicesExistsAliasRequest{Name: []string{exp.cfg.SiteConf.AliasName}}
	res, err := req1.Do(exp.cfg.Ctx, exp.cfgv5.EsClientV5)
	// res, err := ex.cfgv5.EsClientV5.Indices.ExistsAlias([]string{ex.cfg.SiteConf.AliasName})
	if err != nil {
		return err
	}
	if res.StatusCode == http.StatusNotFound {
		log.Println("alias not found, put new alias", exp.cfg.SiteConf.AliasName, " -> ", exp.cfg.FullIndexName)
		//add alias
		req2 := esapi.IndicesPutAliasRequest{Index: []string{exp.cfg.FullIndexName}, Name: exp.cfg.SiteConf.AliasName}
		res, err := req2.Do(exp.cfg.Ctx, exp.cfgv5.EsClientV5)
		// res, err = ex.cfgv5.EsClientV5.Indices.PutAlias([]string{AliasName}, ex.cfg.SiteConf.AliasName)
		if err != nil {
			return err
		}
		if res.IsError() {
			return errors.New("put alias error:" + res.String())
		}
		return nil
	} else if res.IsError() {
		log.Println("IndicesExistsAliasRequest fail:", res.String())
		return errors.New(res.String())
	}
	res.Body.Close()

	log.Println("alias has found")
	// find index name by alias
	req := esapi.IndicesGetAliasRequest{Name: []string{exp.cfg.SiteConf.AliasName}}
	resp, err := req.Do(exp.cfg.Ctx, exp.cfgv5.EsClientV5)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return errors.New("get alias error:" + resp.String())
	}
	body := new(bytes.Buffer)
	_, _ = body.ReadFrom(resp.Body)
	defer resp.Body.Close()

	// resp.String() = [200 OK] {"test_20240912171638":{"aliases":{"test":{}}}}
	// 从已有别名中找出索引名: test_20240912171638
	log.Println("old alias:" + body.String())
	//map
	var bodyMap MapResponse
	err = json.Unmarshal(body.Bytes(), &bodyMap)
	if err != nil {
		return err
	}
	oldIndex := ""
	for index := range bodyMap {
		if strings.HasPrefix(index, ".") {
			continue
		}
		oldIndex = index
	}
	if len(oldIndex) == 0 {
		return errors.New("empty old index name")
	}

	// 别名原子操作
	// 	POST /_aliases
	// {
	//   "actions": [
	//     {
	//       "remove": {
	//         "index": "local_test_202412101212",
	//         "alias": "local_test"
	//       }
	//     },{
	//       "add": {
	//         "index": "local_test_20241210142159",
	//         "alias": "local_test"
	//       }
	//     }
	// ]
	// }

	updateActions := make([]map[string]*UpdateAliasAction, 0)
	removeAction := make(map[string]*UpdateAliasAction)
	removeAction["remove"] = &UpdateAliasAction{
		Index: oldIndex,
		Alias: exp.cfg.SiteConf.AliasName,
	}
	updateActions = append(updateActions, removeAction)

	addAction := make(map[string]*UpdateAliasAction)
	addAction["add"] = &UpdateAliasAction{
		Index: exp.cfg.FullIndexName,
		Alias: exp.cfg.SiteConf.AliasName,
	}
	updateActions = append(updateActions, addAction)

	jsonBody, err := json.Marshal(&UpdateAliasRequest{
		Actions: updateActions,
	})
	if err != nil {
		return errors.New("json fail:" + err.Error())
	}
	if exp.cfg.Debug {
		log.Println(string(jsonBody))
	}

	// make API request
	req3 := esapi.IndicesUpdateAliasesRequest{Body: bytes.NewBuffer(jsonBody)}
	res2, err := req3.Do(exp.cfg.Ctx, exp.cfgv5.EsClientV5)

	if err != nil {
		return errors.New("update alias fail:" + err.Error())
	} else if res2.IsError() {
		return errors.New("update alias error:" + res2.String())
	}
	log.Println(res2.String())
	res2.Body.Close()

	return nil
}

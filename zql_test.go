package zql

import (
	"encoding/json"
	"log"
	"testing"
)

var sql = "select id as aid appname zu_hehe from zu_hehe where (id=1 or name='123') and time>now()-1h group by time(1m) order by id desc limit 10, 10"

//var sql = "select id > aid appname zu_hehe from zu_hehe where (id=1|name='123')&time>now()-1h order by id > desc"

// 基本解析
func Test_zql(t *testing.T) {
	zqlObj, err := New("", sql)
	if err != nil {
		t.Error(err)
	}
	s, _ := json.Marshal(zqlObj)
	log.Println(string(s))
}

// 生成influxdb sql
func Test_influxdb_query(t *testing.T) {
	zqlObj, err := New("", sql)
	if err != nil {
		t.Error(err)
	}
	query, err := zqlObj.GetInfluxdbQuery("group", "hostname", "127.0.0.1")
	if err != nil {
		t.Error(err)
	}
	log.Println(query)
}

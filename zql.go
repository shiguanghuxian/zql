package zql

import (
	"errors"
	"log"
	"sort"
	"strings"
)

func Version() string {
	return "0.1.0"
}

type Zql struct {
	Query   string                  // 查询字符串
	Prefix  string                  // 表前缀
	Select  string                  // 查询字段部分
	From    string                  // 表名
	Where   string                  // 条件部分
	GroupBy string                  // 分组
	OrderBy string                  // 排序部分
	Limit   string                  // 查询结果范围
	Values  *map[string]interface{} // insert 内容部分
}

// select * appname zu_hehe where id = 1 group by time(1m) order by id desc id limit 10,10
func New(prefix, query string) (myZql *Zql, err error) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if query == "" || len(query) < 7 {
		return nil, errors.New("query cannot be empty")
	}
	// 转小写-去空格
	query = strings.TrimSpace(strings.ToLower(query))
	// 创建对象
	myZql = &Zql{
		Query:  query,
		Prefix: prefix,
	}
	// 调用拆分函数-- 判断是插入还是查询
	if query[:6] == "select" {
		myZql.SplitZqlSelectString()
		// 判断字段
		if myZql.Select == "" {
			return nil, errors.New("Query string does not exist 'select'")
		}
	} else if query[:6] == "insert" {
		err = myZql.SplitRegZqlInsertString()
		if err != nil {
			return nil, err
		}
		// 判断values
		if myZql.Values == nil {
			return nil, errors.New("Query string does not exist 'keys|values'")
		}
	} else {
		return nil, errors.New("Query error")
	}
	// 判断表名
	if myZql.From == "" {
		return nil, errors.New("Query string does not exist 'appname|from'")
	}
	// 返回zql对象
	return myZql, nil
}

// 解析sql各部分函数-insert
func (zql *Zql) SplitRegZqlInsertString() error {
	reg := `insert(\s*)into(\s*)(?P<table_name>.*)(\s*)\((?P<keys>.*)\)(\s*)values(\s*)\((?P<values>.*)\)`
	vals, err := RegStrToMap(reg, zql.Query)
	if err != nil {
		return err
	}
	// 获取各部分
	tableName := strings.TrimSpace(vals["table_name"])
	keys := strings.TrimSpace(vals["keys"])
	values := strings.TrimSpace(vals["values"])
	if tableName == "" || keys == "" || values == "" {
		return errors.New("zql 格式错误:有未解析出的关键词")
	}
	// 表名
	zql.From = tableName
	// 健值对
	valuesMap := strings.Split(values, ",")
	keysMap := strings.Split(keys, ",")
	if len(valuesMap) == len(keysMap) {
		valsData := new(map[string]interface{})
		for k, v := range keysMap {
			v = strings.TrimSpace(v)
			vl := strings.TrimSpace(valuesMap[k])
			if vl[:1] == "'" {
				if vl[(len(vl)-1):] != "'" {
					return errors.New("values 格式错误")
				}
				(*valsData)[v] = vl[1:(len(vl) - 1)]
			} else {
				(*valsData)[v] = vl
			}
		}
		zql.Values = valsData
	}
	return nil
}

// 解析sql各部分函数-select
func (zql *Zql) SplitZqlSelectString() {
	var key []int
	val := map[string]int{
		"select":   0,
		"from":     0,
		"appname":  0,
		"where":    0,
		"group by": 0,
		"order by": 0,
		"limit":    0,
	}
	for k, _ := range val {
		index := 0
		switch k {
		case "select", "from", "appname", "where":
			index = strings.Index(zql.Query, k)
			break
		default:
			index = strings.LastIndex(zql.Query, k)
		}
		if index == -1 {
			index = 0
		}
		val[k] = index
		key = append(key, index)
	}
	sort.Ints(key)
	for k, v := range key {
		for kk, vv := range val {
			if vv == 0 && kk != "select" {
				continue
			}
			if vv == v {
				str := ""
				if k == (len(key) - 1) {
					str = zql.Query[(v + len(kk)):]
				} else {
					if key[k+1] == 0 {
						str = zql.Query[(v + len(kk)):]
					} else {
						str = zql.Query[(v + len(kk)):key[k+1]]
					}
				}
				str = strings.TrimSpace(str) // 去空格
				switch kk {
				case "select":
					zql.Select = str
					break
				case "from":
					zql.From = str
					break
				case "appname":
					zql.From = str
					break
				case "where":
					zql.Where = str
					break
				case "group by":
					zql.GroupBy = str
					break
				case "order by":
					zql.OrderBy = str
					break
				case "limit":
					zql.Limit = str
					break
				}

			}
		}
	}

}

/* Insert into插入解析 */
func (zql *Zql) GetInsertIntoData() (data *map[string]interface{}, tableName string) {
	return zql.Values, zql.From
}

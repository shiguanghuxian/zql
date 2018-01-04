package zql

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"gopkg.in/olivere/elastic.v3"
)

// 返回执行结果
func (zql *Zql) GetElasticQuery(client *elastic.Client, dbName string, pretty bool) ([]map[string]interface{}, error) {
	searchSource, err := zql.GetElasticSearchSource()
	if err != nil {
		return make([]map[string]interface{}, 0), err
	}
	result, err := client.Search().
		Index(dbName).               // 数据库名
		Type(zql.Prefix + zql.From). // 表名
		Pretty(pretty).              // 美化输出
		SearchSource(searchSource).  // 条件和聚合信息
		Do()                         // 执行
	if err != nil {
		return make([]map[string]interface{}, 0), err
	}
	// 执行查询出错时
	if result.Error != nil {
		return make([]map[string]interface{}, 0), errors.New(result.Error.RootCause[0].Reason)
	}
	/* 处理返回数据，生成类似数据库数组数据 */
	resultList := make([]map[string]interface{}, 0)
	// 查看聚合数据结果是否为空
	if result.Aggregations != nil {
		buckets := result.Aggregations[zql.GroupBy]
		sjson, err := simplejson.NewJson(*buckets)
		if err != nil {
			return resultList, errors.New("Analytical result set 'json' error")
		}
		bucketsList, err := sjson.Get("buckets").Array()
		for _, v := range bucketsList {
			if valMap, ok := v.(map[string]interface{}); ok == true {
				// 保存每一行数据
				rowMap := make(map[string]interface{}, 0)
				for key, val := range valMap {
					if vv, ok := val.(map[string]interface{}); ok == true {
						rowMap[key] = vv["value"]
					} else {
						rowMap[key] = val
					}
				}
				resultList = append(resultList, rowMap)
			}

		}
	} else if len(result.Hits.Hits) > 0 {
		for _, v := range result.Hits.Hits {
			rowMap := make(map[string]interface{}, 0)
			if sjson, err := simplejson.NewJson(*v.Source); err == nil {
				rowMap, _ = sjson.Map()
			}

			rowMap["_id"] = v.Id // 添加id唯一标识
			resultList = append(resultList, rowMap)
		}
	}

	return resultList, nil
}

// 获取执行构造结果，用于验证-- 相当于orm打印sql
func (zql *Zql) GetElasticQueryStr() (string, error) {
	searchSource, err := zql.GetElasticSearchSource()
	if err != nil {
		return "", err
	}
	dsl, err := searchSource.Source()
	if err != nil {
		return "", err
	}
	js, err := json.Marshal(dsl)
	if err != nil {
		return "", err
	}
	return string(js), nil
}

// 组织整个查询信息
func (zql *Zql) GetElasticSearchSource() (*elastic.SearchSource, error) {
	// 查询构造对象
	searchSource := elastic.NewSearchSource()
	// 条件
	if zql.Where != "" {
		where, err := handleWhereToMapEs(zql.Where)
		if err != nil {
			return nil, errors.New("Field 'where' format error")
		}
		searchSource = searchSource.Query(where) // 查询条件字符串
	}
	// group by
	if zql.GroupBy != "" {
		// 分组情况不需要返回详细列
		searchSource = searchSource.From(0).Size(0)
		// 是否有排序信息
		// order by
		groupByOrderField := ""
		groupByOrderSc := true
		if zql.OrderBy != "" {
			orderBy := strings.Fields(zql.OrderBy)
			if len(orderBy) != 2 {
				orderBy = []string{orderBy[0], "asc"}
			}
			groupByOrderField = strings.TrimSpace(orderBy[0])
			groupByOrderSc = true
			if strings.TrimSpace(orderBy[1]) == "desc" {
				groupByOrderSc = false
			}
		}
		// 定义聚合分组信息
		var aggrDateMain *elastic.DateHistogramAggregation
		var aggrTermsMain *elastic.TermsAggregation
		// 聚合字段
		if strings.Index(zql.GroupBy, "time(") == 0 {
			interval := string(zql.GroupBy[5 : len(zql.GroupBy)-1])
			aggrDateMain = elastic.NewDateHistogramAggregation().Field("date").Interval(interval).Format("yyyy-MM-dd HH:mm:ss")
			if groupByOrderField != "" {
				aggrDateMain.Order(groupByOrderField, groupByOrderSc)
			}
		} else {
			// limit 中获取size
			aggrSize := 100
			if zql.Limit != "" {
				cLimit := strings.Split(zql.Limit, ",")
				if len(cLimit) == 2 {
					aggrSize1, err := strconv.Atoi(cLimit[1])
					if err == nil {
						aggrSize = aggrSize1
					}
				} else {
					aggrSize1, err := strconv.Atoi(cLimit[0])
					if err == nil {
						aggrSize = aggrSize1
					}
				}
			}
			aggrTermsMain = elastic.NewTermsAggregation().Field(zql.GroupBy).Size(aggrSize)
			if groupByOrderField != "" {
				aggrTermsMain.Order(groupByOrderField, groupByOrderSc)
			}
		}
		// 分割select 字段
		selectList := strings.Split(zql.Select, ",")
		// select 聚合函数处理
		for _, v := range selectList {
			// 用as分割字符串
			vval := strings.Split(v, " as ")
			if len(vval) == 2 { // 有as情况
				// as 字段
				asField := strings.TrimSpace(vval[1])
				vField := strings.TrimSpace(vval[0])
				if vField[len(vField)-1:] == ")" {
					// 统计字段
					vFieldVal := strings.TrimSpace(vval[0][strings.Index(vval[0], "(")+1 : strings.Index(vval[0], ")")])
					// 判断是那个聚合函数
					switch vField[:strings.Index(vField, "(")] {
					case "count":
						if vFieldVal == "*" {
							vFieldVal = "_index"
						}
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(asField, elastic.NewValueCountAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(asField, elastic.NewValueCountAggregation().Field(vFieldVal))
						}
						break
					case "avg":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(asField, elastic.NewAvgAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(asField, elastic.NewAvgAggregation().Field(vFieldVal))
						}
						break
					case "sum":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(asField, elastic.NewSumAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(asField, elastic.NewSumAggregation().Field(vFieldVal))
						}
						break
					case "max":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(asField, elastic.NewMaxAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(asField, elastic.NewMaxAggregation().Field(vFieldVal))
						}
						break
					case "min":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(asField, elastic.NewMinAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(asField, elastic.NewMinAggregation().Field(vFieldVal))
						}
						break
					}
				} else {
					// 添加到fields列表
					searchSource.Fields(asField)
					// ScriptField 用于实现as语句
					searchSource.ScriptField(elastic.NewScriptField(asField, elastic.NewScriptInline("doc['"+vField+"'].value")))
				}
				// end 有as情况
			} else if len(vval) == 1 {
				vField := strings.TrimSpace(vval[0])
				// 有聚合函数情况
				if vField[len(vField)-1:] == ")" {
					// 统计字段
					vFieldVal := strings.TrimSpace(vField[strings.Index(vField, "(")+1 : strings.Index(vField, ")")])
					// 判断是那个聚合函数
					switch vField[:strings.Index(vField, "(")] {
					case "count":
						if vFieldVal == "*" {
							vFieldVal = "_index"
						}
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(vField, elastic.NewValueCountAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(vField, elastic.NewValueCountAggregation().Field(vFieldVal))
						}
						break
					case "avg":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(vField, elastic.NewAvgAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(vField, elastic.NewAvgAggregation().Field(vFieldVal))
						}
						break
					case "sum":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(vField, elastic.NewSumAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(vField, elastic.NewSumAggregation().Field(vFieldVal))
						}
						break
					case "max":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(vField, elastic.NewMaxAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(vField, elastic.NewMaxAggregation().Field(vFieldVal))
						}
						break
					case "min":
						if aggrDateMain != nil {
							aggrDateMain.SubAggregation(vField, elastic.NewMinAggregation().Field(vFieldVal))
						} else if aggrTermsMain != nil {
							aggrTermsMain.SubAggregation(vField, elastic.NewMinAggregation().Field(vFieldVal))
						}
						break
					}
				} else {
					// 添加到fields列表
					searchSource.Fields(vField)
				}
			} else {
				return nil, errors.New("Field 'select' format error")
			}
		} // end select 聚合函数处理
		// 分组和聚合信息
		if aggrDateMain != nil {
			searchSource.Aggregation(zql.GroupBy, aggrDateMain)
		} else if aggrTermsMain != nil {
			searchSource.Aggregation(zql.GroupBy, aggrTermsMain)
		}
	}
	// order by
	if zql.OrderBy != "" && zql.GroupBy == "" {
		orderBy := strings.Fields(zql.OrderBy)
		if len(orderBy) != 2 {
			orderBy = []string{orderBy[0], "asc"}
		}
		sc := true
		if strings.TrimSpace(orderBy[1]) == "desc" {
			sc = false
		}
		searchSource = searchSource.Sort(strings.TrimSpace(orderBy[0]), sc)
	}
	// limit
	if zql.Limit != "" && zql.GroupBy == "" {
		limit := strings.Split(zql.Limit, ",")
		if len(limit) > 2 {
			return nil, errors.New("Field 'limit' format error")
		}
		if len(limit) != 2 {
			limit = []string{"0", limit[0]}
		}
		from, err := strconv.Atoi(strings.TrimSpace(limit[0]))
		if err != nil {
			return nil, errors.New("Field 'limit' format error:" + err.Error())
		}
		size, err := strconv.Atoi(strings.TrimSpace(limit[1]))
		if err != nil {
			return nil, errors.New("Field 'limit' format error:" + err.Error())
		}
		searchSource = searchSource.From(from).Size(size)
	}
	// 返回执行信息
	return searchSource, nil
}

// 处理where条件部分
func handleWhereToMapEs(str string) (*elastic.BoolQuery, error) {
	// 拆分数据
	whereList := whrereEs(str)
	//	fmt.Println(whereList)
	// 当前操作符
	key := 0
	// 存储一个or数组
	orWhereArray := elastic.NewBoolQuery()
	for k, v := range whereList {
		if v == "or" { // 分隔每组and条件
			andWhereArray, err := handleAndWhereEs(whereList[key:k])
			if err != nil {
				return nil, err
			}
			key = k + 1
			orWhereArray = orWhereArray.Should(andWhereArray)
		}
		//		fmt.Println(k, v)
	}
	andWhereArray, err := handleAndWhereEs(whereList[key:])
	if err != nil {
		return nil, err
	}
	orWhereArray = orWhereArray.Should(andWhereArray)

	return orWhereArray, nil
}

// and 条件数据
func handleAndWhereEs(andArr []string) (*elastic.BoolQuery, error) {
	// 存储一个and数组
	andWhereArray := elastic.NewBoolQuery()
	// 循环组织条件
	for _, vv := range andArr {
		if vv != "and" && string(vv[0:1]) != "(" {
			// 切割条件下标和表达式
			expression, err := expressionOneWhereEs(vv)
			if err != nil {
				return nil, err
			}
			// 判断值是字符还是数字, #@#@#@#@ 测试时可以去掉测试
			var expVal interface{}
			if string(expression[2][0:1]) == "'" {
				expVal = strings.Trim(expression[2], "'")
			} else {
				expVal, err = strconv.Atoi(expression[2])
				if err != nil {
					// return nil, errors.New("Character conversion digital error:" + expression[2])
					// 判断是否是相对于当前时间的查询
					isDateArr := strings.Split(expression[2], "-")
					if strings.TrimSpace(isDateArr[0]) == "now()" && len(isDateArr) == 2 {
						if expression[0] == "time" {
							expression[0] = "date"
						}
						cha, err := ChaDateTime(isDateArr[1])
						if err != nil {
							expVal = expression[2]
						} else {
							expValDateTime := time.Unix(time.Now().Local().Unix()-cha, 0) // 这里是时间类型，测试后才知道ok否
							expVal = expValDateTime.Format("2006-01-02T15:04:05+08:00")
						}
					} else if strings.Index(strings.TrimSpace(expression[2]), "date(") == 0 {
						if expression[0] == "time" {
							expression[0] = "date"
						}
						// expression[1] = fmt.Sprintf("date-%s", expression[1])
						expValDateStr := strings.Trim(strings.TrimSpace(expression[2][strings.Index(expression[2], "(")+1:strings.Index(expression[2], ")")]), "'")
						// 转为时间格式
						expValDateTime, err := time.Parse("2006-01-02 15:04:05", expValDateStr)
						if err != nil {
							expVal = expValDateStr
						} else {
							expVal = expValDateTime.Format("2006-01-02T15:04:05+08:00")
						}
					} else {
						expVal = expression[2]
					}
				}
			}
			// 这里要判断操作符-从mongodb改的，所以省事在这里在分支一次
			switch expression[1] {
			case "regex":
				andWhereArray = andWhereArray.Must(elastic.NewWildcardQuery(expression[0], fmt.Sprint(expVal)))
				break
			case "eq", "date-eq":
				// andWhereArray = andWhereArray.Must(elastic.NewTermQuery(expression[0], expVal))
				// 短语写法，不知道那种好暂时用上边
				andWhereArray = andWhereArray.Must(elastic.NewMatchPhraseQuery(expression[0], expVal))
				break
			case "neq", "date-neq":
				// andWhereArray = andWhereArray.Must(elastic.NewBoolQuery().MustNot().Must(elastic.NewTermQuery(expression[0], expVal)))
				// 短语写法，不知道那种好暂时用上边
				andWhereArray = andWhereArray.Must(elastic.NewBoolQuery().MustNot(elastic.NewMatchPhraseQuery(expression[0], expVal)))
				break
			case "lte":
				// elastic.NewRangeQuery(expression[0]).From()
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).Lte(expVal))
				break
			case "lt":
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).Lt(expVal))
				break
			case "gte":
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).Gte(expVal))
				break
			case "gt":
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).Gt(expVal))
				break
			case "date-lte":
				andWhereArray = andWhereArray.Must(elastic.NewMatchPhraseQuery(expression[0], expVal))
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).To(expVal))
				break
			case "date-lt":
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).To(expVal))
				break
			case "date-gte":
				andWhereArray = andWhereArray.Must(elastic.NewMatchPhraseQuery(expression[0], expVal))
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).From(expVal))
				break
			case "date-gt":
				andWhereArray = andWhereArray.Must(elastic.NewRangeQuery(expression[0]).From(expVal))
				break
			case "in":
				inList := elasticInList(expVal)
				// 多个列表
				boolIn := elastic.NewBoolQuery()
				for _, v := range inList {
					boolIn = boolIn.Should(elastic.NewMatchPhraseQuery(expression[0], v))
				}
				andWhereArray = andWhereArray.Must(boolIn)
				break
			default:
				andWhereArray.Must(elastic.NewMatchPhraseQuery(expression[0], expVal))
				break
			}
		} else if string(vv[0:1]) == "(" {
			andOne, err := handleWhereToMapEs(vv)
			if err != nil {
				return andWhereArray, err
			}
			andWhereArray = andWhereArray.Must(andOne)
		}
	}
	return andWhereArray, nil
}

// 因为条件一定包含小括号，所以判断第一个字符是否是(就行
func whrereEs(str string) []string {
	// 存储最终数据
	wmap := make([]string, 0)
	// 记录位置
	start := make([]int, 0)
	// 当前操作符
	andor := ""
	// 层级
	cengji := 0
	// 循环
	for k, v := range str {
		if string(v) == "a" {
			// 超出长度
			if (k + 3) >= len(str) {
				continue
			}
			if str[k:(k+3)] == "and" {
				andor = "and"
			}
		} else if string(v) == "o" {
			// 超出长度
			if (k + 2) >= len(str) {
				continue
			}
			if str[k:(k+2)] == "or" {
				andor = "or"
			}
		} else if string(v) == "(" { // 括号分组
			start = append(start, k)
			if andor != "" && len(start) == 1 {
				wmap = append(wmap, andor)
			}
			cengji++
		} else if string(v) == ")" {
			cengji--
			if cengji == 0 {
				wmap = append(wmap, strings.TrimSpace(str[start[0]+1:k]))
			}
			start = start[0 : len(start)-1]
		}
	}
	//	jstr, _ := json.Marshal(wmap)
	//	fmt.Println(string(jstr))
	return wmap
}

// 切割单个条件
func expressionOneWhereEs(str string) ([]string, error) {
	list := strings.Fields(str)
	if len(list) < 3 {
		return make([]string, 0), errors.New("Single condition error:" + str)
	}
	// 取条件位置，防止条件值中出现空格情况
	key := strings.Index(str, list[1]) + len(list[1])
	// 处理条件
	exp := expressionStrEs(list[1])
	// 处理值
	val := strings.TrimSpace(string(str[key:]))
	// 返回数据
	return []string{list[0], exp, val}, nil
}

// 条件替换
func expressionStrEs(str string) string {
	exp := "eq"
	if str == "!=" {
		exp = "neq"
	} else if str == "<=" {
		exp = "lte"
	} else if str == ">=" {
		exp = "gte"
	} else if str == "<" {
		exp = "lt"
	} else if str == ">" {
		exp = "gt"
	} else if str == "=" {
		exp = "eq"
	} else if str == "in" {
		exp = "in"
	} else if str == "like" {
		exp = "regex"
	}
	return exp
}

// 转换in列表
func elasticInList(str interface{}) []string {
	sstr := strings.TrimSpace(fmt.Sprint(str))
	sstr = sstr[1 : len(sstr)-1]
	strList := strings.Split(sstr, ",")
	strList1 := make([]string, 0)
	for _, v := range strList {
		strList1 = append(strList1, strings.Trim(strings.TrimSpace(v), "'"))
	}
	return strList1
}

/*
// 字符串转时间戳秒数
func ChaDateTime(str string) (int64, error) {
	str = strings.TrimSpace(str)
	// 获取各部分
	intSub := str[0 : len(str)-1]
	strSub := str[len(str)-1:]
	// 转数字
	intNum1, err := strconv.Atoi(string(intSub))
	intNum := int64(intNum1)
	if err != nil {
		return 0, err
	}
	// 计算时间
	var outTime int64
	switch strSub {
	case "y":
		outTime = 365 * 24 * 60 * 60
		break
	case "m":
		outTime = 30 * 24 * 60 * 60
		break
	case "d":
		outTime = 24 * 60 * 60
		break
	case "h":
		outTime = 60 * 60
		break
	case "i":
		outTime = 60
		break
	case "s":
		outTime = 1
		break
	}
	return intNum * outTime, nil
}
*/

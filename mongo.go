package zql

// 不再强制转换字段名 2017-01-05
import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// 执行
func (zql *Zql) GetMongoQuery(mgoDb *mgo.Database, subTname string, list *[]map[string]interface{}) error {
	mgoQuery, mgoPipe, _, err := zql.GetMongoQueryDetails(mgoDb, subTname)
	if err != nil {
		return err
	}
	if mgoQuery != nil {
		return mgoQuery.All(list)
	} else if mgoPipe != nil {
		return mgoPipe.All(list)
	}
	return nil
}

// 只返回查询字符串
func (zql *Zql) GetMongoQueryStr(mgoDb *mgo.Database, subTname string) (string, error) {
	_, _, str, err := zql.GetMongoQueryDetails(mgoDb, subTname)
	return str, err
}

// 执行并返回查询字符串
func (zql *Zql) GetMongoQueryDetails(mgoDb *mgo.Database, subTname string) (*mgo.Query, *mgo.Pipe, string, error) {
	if zql.Select == "" || zql.From == "" {
		return nil, nil, "", errors.New("Query string does not exist 'select|from'")
	}
	// fmt.Println(zql.MongodbTableName(zql.Prefix+zql.From, subTname))
	// 构建mongodb查询对象 from
	collection := mgoDb.C(zql.MongodbTableName(zql.Prefix+zql.From, subTname)) // 数据表
	// 根据是否分组查询(group by)区分查询方式
	if zql.GroupBy == "" {
		var mQuery *mgo.Query
		// 判断是否有where条件
		if zql.Where == "" {
			mQuery = collection.Find(bson.M{})
		} else {
			where, err := handleWhereToMap(zql.Where)
			if err != nil {
				return nil, nil, "", err
			} else {
				mQuery = collection.Find(where)
			}
		}
		// 查询字段列表
		if zql.Select != "*" {
			selList := strings.Split(zql.Select, ",")
			selectM := make(bson.M)
			for _, v := range selList {
				selectM[v] = 1
			}
			mQuery.Select(selectM)
		}
		// 判断是否有排序
		if zql.OrderBy != "" {
			// 这里可能有多个排序","分开
			sortList := strings.Split(zql.OrderBy, ",")
			for _, v := range sortList {
				// 按空格分开
				sortExp := strings.Fields(v)
				if len(sortExp) != 2 {
					mQuery.Sort(sortExp[0])
				} else {
					if sortExp[1] == "asc" {
						mQuery.Sort(sortExp[0])
					} else if sortExp[1] == "desc" {
						mQuery.Sort("-" + sortExp[0])
					}
				}
			}
		} else {
			// 不存在排序，则使用时间排序
			mQuery.Sort("datetime")
		}
		// 分页
		if zql.Limit != "" {
			// 切割范围
			limitList := strings.Split(zql.Limit, ",")
			if len(limitList) == 1 {
				limitInt, err := strconv.Atoi(strings.TrimSpace(limitList[0]))
				if err != nil {
					return nil, nil, "", errors.New("Error in 'limit' expression")
				}
				mQuery.Limit(limitInt)
			} else if len(limitList) == 2 {
				skipInt, err := strconv.Atoi(strings.TrimSpace(limitList[0]))
				if err != nil {
					return nil, nil, "", errors.New("Error in 'skip' expression")
				}
				mQuery.Skip(skipInt) // 跳过
				limitInt, err := strconv.Atoi(strings.TrimSpace(limitList[1]))
				if err != nil {
					return nil, nil, "", errors.New("Error in 'limit' expression")
				}
				mQuery.Limit(limitInt) // 查询条数
			}
		}
		//		fmt.Println(mQuery)
		//		err := mQuery.All(list)
		//		fmt.Println(list) // ### 删除
		js, _ := json.Marshal(mQuery)
		// log.Println(string(js))
		//		err := mQuery.All(list)
		return mQuery, nil, string(js), nil
	} else { // group by 情况
		groupBson := make([]bson.M, 0)
		// 判断是否有where条件
		if zql.Where != "" {
			where, err := handleWhereToMap(zql.Where)
			if err != nil {
				return nil, nil, "", err
			} else {
				groupBson = append(groupBson, bson.M{"$match": where})
			}
		}
		// 处理group by 部分
		groupByBson, err := zql.MongoGroupBy()
		if err != nil {
			return nil, nil, "", err
		}
		groupBson = append(groupBson, groupByBson)
		// order by
		if zql.OrderBy != "" {
			oList := strings.Split(zql.OrderBy, ",")
			for _, v := range oList {
				vList := strings.Fields(strings.TrimSpace(v))
				if len(vList) == 2 {
					if vList[1] == "desc" {
						groupBson = append(groupBson, bson.M{"$sort": bson.M{vList[0]: -1}})
					} else {
						groupBson = append(groupBson, bson.M{"$sort": bson.M{vList[0]: 1}})
					}
				} else {
					groupBson = append(groupBson, bson.M{"$sort": bson.M{vList[0]: 1}})
				}
			}
		}
		// limit
		if zql.Limit != "" {
			lList := strings.Split(zql.Limit, ",")
			limit1, err := strconv.Atoi(strings.TrimSpace(lList[0]))
			if err != nil {
				return nil, nil, "", errors.New("Query keywords 'limit' error")
			}
			if len(lList) == 2 {
				limit2, err := strconv.Atoi(strings.TrimSpace(lList[1]))
				if err != nil {
					return nil, nil, "", errors.New("Query keywords 'limit' error")
				}
				groupBson = append(groupBson, bson.M{"$skip": limit1})  // 跳过文档数
				groupBson = append(groupBson, bson.M{"$limit": limit2}) // 查询文档数
			} else {
				groupBson = append(groupBson, bson.M{"$limit": limit1}) // 查询文档数
			}
		}
		js, _ := json.Marshal(groupBson)
		//		err = collection.Pipe(groupBson).All(list)
		return nil, collection.Pipe(groupBson), string(js), err
	}

	return nil, nil, "", nil
}

// 处理group by
func (zql *Zql) MongoGroupBy() (bson.M, error) {
	group := make(bson.M, 0)
	// 判断分组是否是按时间分组
	if strings.Index(zql.GroupBy, "time(") == 0 {
		// 获取时间
		stepTime, err := ChaDateTime(string(zql.GroupBy[5 : len(zql.GroupBy)-1]))
		if err != nil {
			return bson.M{}, errors.New("Query keywords 'group by' error")
		}
		group = bson.M{
			"_id": bson.M{
				"$subtract": []bson.M{
					bson.M{
						"$divide": []interface{}{"$datetime", stepTime},
					},
					bson.M{
						"$mod": []interface{}{
							bson.M{
								"$divide": []interface{}{"$datetime", stepTime},
							},
							1,
						},
					},
				},
			},
		}
	} else {
		group = bson.M{"_id": "$" + zql.GroupBy}
	}
	if zql.Select == "*" {
		return group, errors.New("'group by' query field can not be '*'")
	}
	// 从select中获取要显示和聚合函数
	selectList := strings.Split(zql.Select, ",")
	for _, v := range selectList {
		fields := strings.Split(strings.TrimSpace(v), " as ")
		key, val := fieldsAggregationName(fields[0])
		//		fmt.Println(key, val)
		// 组织bson
		if len(fields) == 2 {
			if key == "$count" {
				group[fields[1]] = bson.M{"$sum": 1}
			} else {
				group[fields[1]] = bson.M{key: val}
			}
		} else {
			if key == "$count" {
				group["doc_count"] = bson.M{"$sum": 1}
			} else {
				group[string(val[1:])] = bson.M{key: val}
			}
		}
	}
	// 分组条件
	// js, _ := json.Marshal(group)
	// log.Println(string(js))
	return bson.M{"$group": group}, nil
}

// 获取select fields转成对应聚合函数
func fieldsAggregationName(str string) (string, string) {
	str = strings.TrimSpace(str)
	// 看字符串中是否包含（
	if strings.Index(str, "(") >= 2 {
		return "$" + string(str[:strings.Index(str, "(")]), "$" + string(str[strings.Index(str, "(")+1:len(str)-1])
	} else {
		return "$first", "$" + str
	}
	return "$first", "$" + str
}

// 将where字符串转成mongodb bson条件
func handleWhereToMap(str string) (bson.M, error) {
	// 拆分数据
	whereList := whrere(str)
	// 当前操作符
	key := 0
	// 存储一个or数组
	orWhereArray := make([]bson.M, 0)
	for k, v := range whereList {
		if v == "or" { // 分隔每组and条件
			andWhereArray, err := handleAndWhere(whereList[key:k])
			if err != nil {
				return nil, err
			}
			key = k + 1
			orWhereArray = append(orWhereArray, bson.M{"$and": andWhereArray})
		}
		//		fmt.Println(k, v)
	}
	andWhereArray, err := handleAndWhere(whereList[key:])
	if err != nil {
		return nil, err
	}
	orWhereArray = append(orWhereArray, bson.M{"$and": andWhereArray})

	return bson.M{"$or": orWhereArray}, nil
}

// and 条件数据
func handleAndWhere(andArr []string) ([]bson.M, error) {
	// 存储一个and数组
	andWhereArray := make([]bson.M, 0)
	// 循环组织条件
	for _, vv := range andArr {
		if vv != "and" && string(vv[0:1]) != "(" {
			// 切割条件下标和表达式
			expression, err := expressionOneWhere(vv)
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
						// 如果用户输入的字段时time，这里强制成datetime
						if expression[0] == "time" {
							expression[0] = "datetime"
						}
						cha, err := ChaDateTime(isDateArr[1])
						if err != nil {
							expVal = expression[2]
						} else {
							// 时间比较字段使用时间格式没有成功，这里使用时间戳字段
							expVal = time.Now().Local().Unix() - cha // "ISODate('" + time.Unix(time.Now().Local().Unix()-cha, 0).Format("2006-01-02 15:04:05") + "')"
						}
					} else if strings.Index(strings.TrimSpace(expression[2]), "date(") == 0 {
						// 如果用户输入的字段时time，这里强制成datetime
						if expression[0] == "time" {
							expression[0] = "datetime"
						}
						dateStr := strings.Trim(strings.TrimSpace(expression[2][strings.Index(expression[2], "(")+1:strings.Index(expression[2], ")")]), "'")
						dateStrTime, err := time.ParseInLocation("2006-01-02 15:04:05", dateStr, time.Local)
						if err != nil {
							return nil, errors.New("Query keywords 'where' error:" + err.Error())
						}
						expVal = dateStrTime.Unix()
						// 如果用户输入的字段时time，这里强制成datetime
						if expression[0] == "time" {
							expression[0] = "datetime"
						}
					} else {
						expVal = expression[2]
					}
				}
			}
			if expression[1] == "$regex" {
				andWhereArray = append(andWhereArray, bson.M{expression[0]: bson.M{expression[1]: expVal, "$options": "$i"}})
			} else if expression[1] == "$in" {
				andWhereArray = append(andWhereArray, bson.M{expression[0]: bson.M{expression[1]: mongoInList(expVal)}})
			} else {
				andWhereArray = append(andWhereArray, bson.M{expression[0]: bson.M{expression[1]: expVal}})
			}
		} else if string(vv[0:1]) == "(" {
			andOne, err := handleWhereToMap(vv)
			if err != nil {
				return andWhereArray, err
			}
			andWhereArray = append(andWhereArray, andOne)
		}
	}
	return andWhereArray, nil
}

// 因为条件一定包含小括号，所以判断第一个字符是否是(就行
func whrere(str string) []string {
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
func expressionOneWhere(str string) ([]string, error) {
	list := strings.Fields(str)
	if len(list) < 3 {
		return make([]string, 0), errors.New("Single condition error:" + str)
	}
	// 取条件位置，防止条件值中出现空格情况
	key := strings.Index(str, list[1]) + len(list[1])
	// 处理条件
	exp := expressionStr(list[1])
	// 处理值
	val := strings.TrimSpace(string(str[key:]))
	// 返回数据
	return []string{list[0], exp, val}, nil
}

// 条件替换
func expressionStr(str string) string {
	exp := "$ne"
	if str == "!=" {
		exp = "$ne"
	} else if str == "<=" {
		exp = "$lte"
	} else if str == ">=" {
		exp = "$gte"
	} else if str == "<" {
		exp = "$lt"
	} else if str == ">" {
		exp = "$gt"
	} else if str == "=" {
		exp = "$eq"
	} else if str == "in" {
		exp = "$in"
	} else if str == "like" {
		exp = "$regex"
	}
	return exp
}

// 格式化表名
func (zql *Zql) MongodbTableName(tname, subTname string) string {
	if subTname == "" {
		return tname
	}
	return tname + "_" + subTname
}

// 转换in列表
func mongoInList(str interface{}) []string {
	sstr := strings.TrimSpace(fmt.Sprint(str))
	sstr = sstr[1 : len(sstr)-1]
	strList := strings.Split(sstr, ",")
	strList1 := make([]string, 0)
	for _, v := range strList {
		strList1 = append(strList1, strings.Trim(strings.TrimSpace(v), "'"))
	}
	return strList1
}

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
	case "M":
		outTime = 30 * 24 * 60 * 60
		break
	case "d":
		outTime = 24 * 60 * 60
		break
	case "h":
		outTime = 60 * 60
		break
	case "m":
		outTime = 60
		break
	case "s":
		outTime = 1
		break
	}
	return intNum * outTime, nil
}

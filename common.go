package zql

import (
	"errors"
	"regexp"
)

// 正则解析 `(?P<abc>Hello)(.*)(?P<cba>Go).`
func RegStrToMap(regStr string, str string) (map[string]string, error) {
	// 正则对象
	reg := regexp.MustCompile(regStr)
	// 所有定义的下标
	names := reg.SubexpNames()
	// 返回数据map
	strMap := make(map[string]string)
	// 取数据
	i := 0
	for _, v := range names {
		if v == "" {
			continue
		}
		i++
		// 获取单个值
		strOne := reg.ReplaceAllString(str, "$"+v)
		// 当有一个未匹配到会出现所有都是原字符串
		if strOne == str {
			continue
		}
		strMap[v] = strOne
	}
	if len(strMap) < i {
		return strMap, errors.New("未匹配全部属性")
	}
	return strMap, nil
}

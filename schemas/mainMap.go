package schemas

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
)

type MainMap struct {
	INTEGER_MAP       map[string]int64
	INTEGER_ARRAY_MAP map[string][]int64
	STRING_MAP        map[string]string
	STRING_ARRAY_MAP  map[string][]string
	FLOAT_MAP         map[string]float64
	FLOAT_ARRAY_MAP   map[string][]float64
	TotalKeys         int
}

func CreateMainMap() *MainMap {
	return &MainMap{
		INTEGER_MAP:       make(map[string]int64),
		STRING_MAP:        make(map[string]string),
		INTEGER_ARRAY_MAP: make(map[string][]int64),
		STRING_ARRAY_MAP:  make(map[string][]string),
		FLOAT_MAP:         make(map[string]float64),
		FLOAT_ARRAY_MAP:   make(map[string][]float64),
		TotalKeys:         0,
	}
}

func (m *MainMap) SetInteger(key string, value int64) {
	zap.L().Info("Setting Integer", zap.String("key", key), zap.Int64("value", value))
	m.INTEGER_MAP[key] = value

}

func (m *MainMap) SetString(key string, value string) {
	zap.L().Info("Setting String", zap.String("key", key), zap.String("value", value))
	m.STRING_MAP[key] = value

}

func (m *MainMap) SetIntegerArray(key string, value []int64) {
	zap.L().Info("Setting Integer Array", zap.String("key", key), zap.Int64s("value", value))
	m.INTEGER_ARRAY_MAP[key] = value

}

func (m *MainMap) SetStringArray(key string, value []string) {
	zap.L().Info("Setting String Array", zap.String("key", key), zap.Strings("value", value))
	m.STRING_ARRAY_MAP[key] = value

}
func (m *MainMap) SetFloat(key string, value float64) {
	zap.L().Info("Setting Float", zap.String("key", key), zap.Float64("value", value))
	m.FLOAT_MAP[key] = value

}

func (m *MainMap) SetFloatArray(key string, value []float64) {
	zap.L().Info("Setting Float Araay", zap.String("key", key), zap.Float64s("value", value))
	m.FLOAT_ARRAY_MAP[key] = value
}

func (m *MainMap) getValue(key string) interface{} {
	if stringValue, ok := m.STRING_MAP[key]; ok {
		return stringValue
	}
	if stringArrayValue, ok := m.STRING_ARRAY_MAP[key]; ok {
		return stringArrayValue
	}
	if intValue, ok := m.INTEGER_MAP[key]; ok {
		return intValue
	}
	if intArrayValue, ok := m.INTEGER_ARRAY_MAP[key]; ok {
		return intArrayValue
	}
	if floatValue, ok := m.FLOAT_MAP[key]; ok {
		return floatValue
	}
	if floatArrayValue, ok := m.FLOAT_ARRAY_MAP[key]; ok {
		return floatArrayValue
	}
	return nil
}

func (m *MainMap) Print() {
	jsonData, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting MainMap to JSON: %v\n", err)
		return
	}
	fmt.Println(string(jsonData))
}

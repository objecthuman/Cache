package main

import (
	"fmt"
	"go.uber.org/zap"
	"in-memory-store/constants"
	"in-memory-store/schemas"
	"in-memory-store/snapshots"
)

func SetValue(m *schemas.MainMap, key string, value interface{}) {
	switch v := value.(type) {
	case int64:
		m.SetInteger(key, v)
	case string:
		m.SetString(key, v)
	case float64:
		m.SetFloat(key, v)
	case []int64:
		m.SetIntegerArray(key, v)
	case []string:
		m.SetStringArray(key, v)
	case []float64:
		m.SetFloatArray(key, v)
	default:
		zap.L().Warn("Unsupported value type", zap.String("key", key), zap.Any("value", value))
		return
	}
	m.TotalNoOfOperations++
	if (m.TotalNoOfOperations % constants.RUN_SNAPSHOT_AFTER) == 0 {
		snapshots.RunSnapShotTaker(m)
	}
}
func main() {
	logger := GetLogger()
	defer logger.Sync()
	globalMap := schemas.CreateMainMap()
	defer snapshots.RunSnapShotTaker(globalMap)
	logger.Info("Application Initilized")
	snapshots.ReadSnapShotFile(globalMap)
	fmt.Println(globalMap)

	SetValue(globalMap, "thisisveryveryveryveryveryverylong", int64(458234092380598235))
	SetValue(globalMap, "minusOne", int64(-1))
	SetValue(globalMap, "plus2", int64(2))
	SetValue(globalMap, "plus3", int64(3))
	SetValue(globalMap, "plus10", int64(10))
	SetValue(globalMap, "one hundred", int64(100))
	SetValue(globalMap, "hehehehe", int64(69))
	SetValue(globalMap, "firstString", "this is the value of the first string")
	SetValue(globalMap, "New string", "new string")
	arr := []int64{10, 20, 30, 40, 50}
	SetValue(globalMap, "int arr", arr)
	arr2 := []int64{80, 90, 500, 200, 80808080}
	SetValue(globalMap, "int arr12", arr2)
	floatArray := []float64{69.69, 20.22, 33.33}
	SetValue(globalMap, "nirajanArray", floatArray)
	SetValue(globalMap, "nirajanFloat", float64(69.69))
	stringArray := []string{"hello", "how are you", "hehehhehehh"}
	SetValue(globalMap, "str array", stringArray)
	stringArray2 := []string{"brrr", "fff"}
	SetValue(globalMap, "str212", stringArray2)
	logger.Info("Application Closing....")

	logger.Info("Application Closing....")
}

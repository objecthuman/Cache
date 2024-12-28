package snapshots

import (
	"bytes"
	"encoding/binary"
	"go.uber.org/zap"
	"in-memory-store/constants"
	"in-memory-store/schemas"
	"os"
	"sync"
)

func createFileHeader() (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	if _, err := buffer.WriteString(constants.FILE_HEADER); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, constants.CURRENT_VERSION); err != nil {
		return nil, err
	}
	return &buffer, nil
}

func convertStringMapToBin(mainMap *schemas.MainMap) ([]byte, error) {
	stringMap := mainMap.STRING_MAP
	var buffer bytes.Buffer
	for key, value := range stringMap {
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(constants.STRING_TYPE)); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the length of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(valueBytes))); err != nil {
			return nil, err
		}
		// write the actual value
		if err := binary.Write(&buffer, binary.LittleEndian, (valueBytes)); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}

func convertStringArrayMapToBin(mainMap *schemas.MainMap) ([]byte, error) {
	stringMap := mainMap.STRING_ARRAY_MAP
	var buffer bytes.Buffer

	for key, stringArray := range stringMap {
		keyBytes := []byte(key)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, constants.STRING_ARRAY_TYPE); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the length of string array
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(stringArray))); err != nil {
			return nil, err
		}
		for _, stringValue := range stringArray {
			// write the length of the string value
			valueBytes := []byte(stringValue)
			if err := binary.Write(&buffer, binary.LittleEndian, int64(len(valueBytes))); err != nil {
				return nil, err
			}
			// write the actual value
			if err := binary.Write(&buffer, binary.LittleEndian, (valueBytes)); err != nil {
				return nil, err
			}
			buffer.WriteByte(byte(0))
		}
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}
func convertIntegerMapToBinary(minmap *schemas.MainMap) ([]byte, error) {
	var buffer bytes.Buffer
	integerMap := minmap.INTEGER_MAP
	for key, value := range integerMap {
		keyBytes := []byte(key)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(constants.INTEGER_ARRAY_TYPE)); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the actual value
		if err := binary.Write(&buffer, binary.LittleEndian, (value)); err != nil {
			return nil, err
		}
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}

func convertIntegerArrayMapToBinary(minmap *schemas.MainMap) ([]byte, error) {
	var buffer bytes.Buffer
	integerArray := minmap.INTEGER_ARRAY_MAP
	for key, value := range integerArray {
		keyBytes := []byte(key)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(constants.INTEGER_ARRAY_TYPE)); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the length of integer array
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(value))); err != nil {
			return nil, err
		}
		// write the actual value
		if err := binary.Write(&buffer, binary.LittleEndian, value); err != nil {
			return nil, err
		}
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}

func convertFloatMapToBinary(minmap *schemas.MainMap) ([]byte, error) {
	var buffer bytes.Buffer
	floatMap := minmap.FLOAT_MAP
	for key, value := range floatMap {
		keyBytes := []byte(key)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(constants.FLOAT_TYPE)); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the actual value
		if err := binary.Write(&buffer, binary.LittleEndian, (value)); err != nil {
			return nil, err
		}
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}

func convertFloatArrayMapToBinary(minmap *schemas.MainMap) ([]byte, error) {
	var buffer bytes.Buffer
	floatArray := minmap.FLOAT_ARRAY_MAP
	for key, value := range floatArray {
		keyBytes := []byte(key)

		// write the type of the value
		if err := binary.Write(&buffer, binary.LittleEndian, int64(constants.FLOAT_ARRAY_TYPE)); err != nil {
			return nil, err
		}
		// write the length of the key
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(keyBytes))); err != nil {
			return nil, err
		}
		// write the key
		if err := binary.Write(&buffer, binary.LittleEndian, keyBytes); err != nil {
			return nil, err
		}
		buffer.WriteByte(byte(0))
		// write the length of float array
		if err := binary.Write(&buffer, binary.LittleEndian, int64(len(value))); err != nil {
			return nil, err
		}
		// write the actual value
		if err := binary.Write(&buffer, binary.LittleEndian, value); err != nil {
			return nil, err
		}
		buffer.WriteString("\r\n")
	}
	return buffer.Bytes(), nil
}
func createBytesForSnapShot(mainMap *schemas.MainMap) *bytes.Buffer {
	mainBuffer, err := createFileHeader()
	if err != nil {
		zap.L().Error("Failed to create file header", zap.Error(err))
	}
	// write integer bytes
	integerBinBytes, err := convertIntegerMapToBinary(mainMap)
	if err != nil {
		zap.L().Error("Failed creating integer map bin", zap.Error(err))
	}
	mainBuffer.Write(integerBinBytes)
	// write integer array bytes
	integerArrayBytes, err := convertIntegerArrayMapToBinary(mainMap)
	if err != nil {
		zap.L().Error("Failed creating integer array map bin", zap.Error(err))
	}
	mainBuffer.Write(integerArrayBytes)
	// write string bytes
	stringBinBytes, err := convertStringMapToBin(mainMap)
	if err != nil {
		zap.L().Error("Failed creating string map bin", zap.Error(err))
	}
	mainBuffer.Write(stringBinBytes)
	// write string array bytes
	stringArrayBinBytes, err := convertStringArrayMapToBin(mainMap)
	if err != nil {
		zap.L().Error("Failed creating string array map bin", zap.Error(err))
	}
	mainBuffer.Write(stringArrayBinBytes)
	// write float bytes
	floatBinBytes, err := convertFloatMapToBinary(mainMap)
	if err != nil {
		zap.L().Error("Failed creating float map bin", zap.Error(err))
	}
	mainBuffer.Write(floatBinBytes)
	// write float array bytes
	floatArrayBin, err := convertFloatArrayMapToBinary(mainMap)
	if err != nil {
		zap.L().Error("Failed creating float array map bin", zap.Error(err))
	}
	mainBuffer.Write(floatArrayBin)
	return mainBuffer
}
func takeSnapShot(wg *sync.WaitGroup, mainMap *schemas.MainMap) {
	defer wg.Done()
	file, err := os.Create(constants.SNAPSHOT_FILE_NAME)
	if err != nil {
		zap.L().Error("Error while taking snapshot of file", zap.Error(err))
	}
	buffer := createBytesForSnapShot(mainMap)
	bytes := buffer.Bytes()
	file.Write(bytes)
	zap.L().Info("Snapshot taken successfully")
	file.Close()
}
func RunSnapShotTaker(mainMap *schemas.MainMap) {
	var wg sync.WaitGroup
	wg.Add(1)
	go takeSnapShot(&wg, mainMap)
	wg.Wait()
}

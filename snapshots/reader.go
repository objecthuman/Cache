package snapshots

import (
	"encoding/binary"
	"fmt"
	"in-memory-store/constants"
	"in-memory-store/schemas"
	"io"
	"math"
	"os"

	"go.uber.org/zap"
)

type BinaryReader struct {
	file *os.File
}

func CreateBinaryReader(file *os.File) BinaryReader {
	return BinaryReader{
		file: file,
	}
}

func (reader *BinaryReader) skipFileHeader() error {
	bytes := make([]byte, len(constants.FILE_HEADER))
	l, err := reader.file.Read(bytes)
	if err != nil {
		return err
	}
	if l < len(constants.FILE_HEADER) {
		return fmt.Errorf("Error while skipping file header expected %d bytes found %d", len(constants.FILE_HEADER), l)
	}
	header_contents := string(bytes)
	if header_contents != constants.FILE_HEADER {
		return fmt.Errorf("Invalid file header found, expected %s, found %s", constants.FILE_HEADER, header_contents)
	}
	return err
}

func (reader *BinaryReader) skipBlockSeperator() error {
	bytes := make([]byte, constants.BLOCK_SEPERATOR_LENGTH)
	l, err := reader.file.Read(bytes)
	if err != nil {
		return err
	}
	if l < constants.BLOCK_SEPERATOR_LENGTH {
		return fmt.Errorf("Error skipping block seperator not enough bytes found")
	}
	blockSeperatorString := string(bytes)
	if blockSeperatorString != "\r\n" {
		return fmt.Errorf("Block seperator expected '\r\n' found %s", blockSeperatorString)
	}
	return nil
}

func (reader *BinaryReader) getInt64DataFromBlock() (int64, error) {
	bytes := make([]byte, constants.INT_TYPE_LENGTH)
	l, err := reader.file.Read(bytes)
	if err != nil {
		return 0, err
	}
	if l < constants.INT_TYPE_LENGTH {
		return 0, fmt.Errorf("Expected 8 bytes but found only %d while reading integer", l)
	}

	return int64(binary.LittleEndian.Uint64(bytes)), nil
}
func (reader *BinaryReader) getInt64ArrayDataFromBlock(length int64) ([]int64, error) {
	bytes := make([]byte, length*int64(constants.INT_TYPE_LENGTH))
	l, err := reader.file.Read(bytes)
	if err != nil {
		return nil, err
	}
	if l < int(length) {
		return nil, fmt.Errorf("Expected 8 bytes but found only %d while reading integer", l)
	}
	var int64Array []int64
	for i := 0; i < int(length*int64(constants.INT_TYPE_LENGTH)); i += 8 {
		int64Value := int64(binary.LittleEndian.Uint64(bytes[i : i+8]))
		int64Array = append(int64Array, int64Value)
	}

	return int64Array, nil
}

func (reader *BinaryReader) getFloat64DataFromBlock() (float64, error) {
	bytes := make([]byte, constants.FLOAT_TYPE_LENGTH)
	l, err := reader.file.Read(bytes)
	if err != nil {
		return 0, err
	}
	if l < constants.FLOAT_TYPE_LENGTH {
		return 0, fmt.Errorf("Expected 8 bytes but found only %d while reading float", l)
	}
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits), nil
}
func (reader *BinaryReader) getFloat64ArrayDataFromBlock(length int64) ([]float64, error) {
	bytes := make([]byte, length*int64(constants.FLOAT_TYPE_LENGTH))
	l, err := reader.file.Read(bytes)
	if err != nil {
		return nil, err
	}
	if l < int(length) {
		return nil, fmt.Errorf("Expected 8 bytes but found only %d while reading float", l)
	}
	var float64Array []float64
	for i := 0; i < int(length*int64(constants.FLOAT_TYPE_LENGTH)); i += 8 {
		bits := binary.LittleEndian.Uint64(bytes[i : i+8])
		float64Array = append(float64Array, math.Float64frombits(bits))
	}

	return float64Array, nil

}

func (reader *BinaryReader) getStringDataFromBlock(stringLength int64) (string, error) {
	bytes := make([]byte, stringLength+1)
	l, err := reader.file.Read(bytes)
	if err != nil {
		return "", err
	}
	if l < int(stringLength+1) {
		return "", fmt.Errorf("Expected %d bytes but found only %d while reading string", stringLength, l)
	}
	return string(bytes[:stringLength]), err
}
func handleError(err error, context string) bool {
	if err == io.EOF {
		return true
	}
	if err != nil {
		zap.L().Error(context, zap.Error(err))
		return true
	}
	return false
}

func ReadSnapShotFile(mainMap *schemas.MainMap) {
	f, err := os.Open(constants.SNAPSHOT_FILE_NAME)
	defer f.Close()
	if err != nil {
		zap.L().Warn("Error reading snapshot file", zap.Error(err))
		return
	}
	reader := CreateBinaryReader(f)
	err = reader.skipFileHeader()
	if err != nil {
		zap.L().Error("Error skipping file header", zap.Error(err))
	}
	version, err := reader.getInt64DataFromBlock()
	if handleError(err, "Error while reading version") {
		return
	}
	zap.L().Info("Reading snapshot file", zap.Int64("version", version))
	for {
		blockValueType, err := reader.getInt64DataFromBlock()
		if handleError(err, "Error while reading block type") {
			return
		}
		keyLength, err := reader.getInt64DataFromBlock()
		if handleError(err, "Error while reading key length") {
			return
		}

		key, err := reader.getStringDataFromBlock(keyLength)
		if handleError(err, "Error while reading key") {
			return
		}
		switch blockValueType {
		case constants.INTEGER_TYPE:
			blockValue, err := reader.getInt64DataFromBlock()
			if handleError(err, "Error while reading integer block value") {
				return
			}
			mainMap.SetInteger(key, blockValue)
		case constants.STRING_TYPE:
			valueLength, err := reader.getInt64DataFromBlock()
			if handleError(err, "Error while reading string length for block value") {
				return
			}
			blockValue, err := reader.getStringDataFromBlock(valueLength)
			if handleError(err, "Error while reading block value") {
				return
			}
			mainMap.SetString(key, blockValue)
		case constants.INTEGER_ARRAY_TYPE:
			valueLength, err := reader.getInt64DataFromBlock()
			if handleError(err, "Error while reading integer array length for block value") {
				return
			}
			blockValue, err := reader.getInt64ArrayDataFromBlock(valueLength)
			if handleError(err, "Error while reading integer array block value") {
				return
			}
			mainMap.SetIntegerArray(key, blockValue)
		case constants.FLOAT_TYPE:
			blockValue, err := reader.getFloat64DataFromBlock()
			if handleError(err, "Error while reading float block value") {
				return
			}
			mainMap.SetFloat(key, blockValue)
		case constants.FLOAT_ARRAY_TYPE:
			valueLength, err := reader.getInt64DataFromBlock()
			fmt.Printf("length of float array %d", valueLength)
			if handleError(err, "Error while reading float array length for block value") {
				return
			}
			blockValue, err := reader.getFloat64ArrayDataFromBlock(valueLength)
			if handleError(err, "Error while reading float array block value") {
				return
			}
			mainMap.SetFloatArray(key, blockValue)
		}
		err = reader.skipBlockSeperator()
		if handleError(err, "Error while skipping block") {
			return
		}
	}
}

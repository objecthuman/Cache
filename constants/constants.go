package constants

var SNAPSHOT_FILE_NAME string = "snapshot"

var DEFAULT_SNAPSHOT_INTERVAL = 10

var STRING_TYPE int64 = 0x01
var STRING_ARRAY_TYPE int64 = 0x02
var INTEGER_TYPE int64 = 0x03
var INTEGER_ARRAY_TYPE int64 = 0x04
var FLOAT_TYPE int64 = 0x05
var FLOAT_ARRAY_TYPE int64 = 0x06

var FILE_HEADER string = "CerebralCache"

var END_OF_FILE int32 = -1

var INT_TYPE_LENGTH = 8

var FLOAT_TYPE_LENGTH = 8

var BLOCK_SEPERATOR_LENGTH = 2

var CURRENT_VERSION int64 = 1

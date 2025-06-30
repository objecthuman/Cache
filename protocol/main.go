package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"go.uber.org/zap"
)

const (
	Create uint64 = 0
	Delete uint64 = 1
)

var Version = []uint8{0, 0, 0}

func convertBytesToUnit8(bytes []byte) []uint8 {
	uint8Array := []uint8{}
	for _, b := range bytes {
		uint8Array = append(uint8Array, uint8(b))
	}
	return uint8Array
}

func convertVersionToString(version []uint8) string {
	versionString := ""
	for i, ver := range version {
		if i > 0 {
			versionString = fmt.Sprintf("%s.%d", versionString, ver)
		} else {
			versionString = fmt.Sprintf("%d", uint8(ver))
		}
	}
	return versionString

}

func compareVersion(incomingVersion []uint8) bool {
	if len(Version) != len(incomingVersion) {
		return false
	}
	for index, val := range Version {
		if val != incomingVersion[index] {
			return false
		}
	}
	return true

}

func readExactBytes(reader *bufio.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read %d bytes: %w", n, err)
	}
	return buf, nil

}

func acceptConnection(client net.Conn) {
	reader := bufio.NewReader(client)
	for {
		versionBuffer, err := readExactBytes(reader, 3)
		if err != nil {
			zap.L().Error("Failed reading version from client", zap.Error(err))
			continue
		}

		versionUint8 := convertBytesToUnit8(versionBuffer)
		if !compareVersion(versionUint8) {
			zap.L().Error("Client and server version mismatch",
				zap.String("Server version", convertVersionToString(Version)),
				zap.String("Client version", convertVersionToString(versionUint8)),
			)
			continue
		}

		contentLengthBuffer, err := readExactBytes(reader, 8)
		if err != nil {
			zap.L().Error("Failed reading content length", zap.Error(err))
			continue
		}
		contentLength := binary.BigEndian.Uint64(contentLengthBuffer)

		content, err := readExactBytes(reader, int(contentLength))
		if err != nil {
			zap.L().Error("Failed reading content",
				zap.Uint64("Expected content length", contentLength),
				zap.Error(err),
			)
			return
		}
		actionType, err := readExactBytes(reader, 8)
		if err != nil {
			zap.L().Error("Failed reading action type",
				zap.Uint8("Expected action type length", 8),
				zap.Error(err),
			)
			continue
		}
	}
}

func startServer() {
	server, err := net.Listen("tcp4", "localhost:4444")
	if err != nil {
		zap.L().Error("failed starting server", zap.Error(err))
		return
	}
	defer server.Close()
	for {
		client, err := server.Accept()
		if err != nil {
			zap.L().Error("failed accepting connection from client", zap.Error(err))
		}
		go func() {
			acceptConnection(client)
			defer client.Close()
		}()
	}
}

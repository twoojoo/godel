package client

import (
	"crypto/rand"
	"encoding/binary"
)

func generateCorrelationID() (int32, error) {
	var b [84]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b[:])), nil
}

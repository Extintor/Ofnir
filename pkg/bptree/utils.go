package bptree

import (
	"bytes"
	"encoding/binary"
)

func insert[T any](lt []T, index int, t T) []T {
	if len(lt) == index {
		return append(lt, t)
	}
	lt = append(lt[:index+1], lt[index:]...)
	lt[index] = t
	return lt
}

func byteArrayFromOrd(i int) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, int16(i))
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func byteArraySliceFromRange(start, end int) [][]byte {
	var r [][]byte
	for x := start; x <= end; x++ {
		r = append(r, byteArrayFromOrd(x))
	}
	return r
}

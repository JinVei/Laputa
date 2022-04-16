package common

import (
	"errors"
	"strconv"
	"strings"
)

func GetLogName(number uint64) string {
	return strconv.Itoa(int(number)) + ".log"
}

func GetManifestName(number uint64) string {
	return strconv.Itoa(int(number)) + ".manifest"
}

func GetManifestNameBytes(number []byte) string {
	return string(number) + ".manifest"
}

func GetTableName(number uint64) string {
	return strconv.Itoa(int(number)) + ".sst"
}

func GetCurrentName() string {
	return "CURRENT"
}

func LogNameToNumber(name string) uint64 {
	//strings.Split(name,".log")
	logstr := strings.TrimSuffix(name, ".log")
	number, _ := strconv.Atoi(logstr)
	return uint64(number)
}

func IsLogFile(name string) bool {
	return strings.HasSuffix(name, ".log")
}

func IsManifestFile(name string) bool {
	return strings.HasSuffix(name, ".manifest")
}

func IsSstFile(name string) bool {
	return strings.HasSuffix(name, ".sst")
}

func ParseFileNameNumber(name string) int {
	var number int
	var err error
	if IsLogFile(name) {
		number, err = strconv.Atoi(strings.TrimSuffix(name, ".log"))
	} else if IsManifestFile(name) {
		number, err = strconv.Atoi(strings.TrimSuffix(name, ".manifest"))
	} else if IsSstFile(name) {
		number, err = strconv.Atoi(strings.TrimSuffix(name, ".sst"))
	} else {
		err = errors.New("Unknow File")
	}
	if err != nil {
		number = -1
	}
	return number
}

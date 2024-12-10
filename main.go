package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type blob struct {
	FileName string  `json:"filename"`
	Chunks   []chunk `json:"chunks"`
	Version  string  `json:"version"`
}

type chunk struct {
	Id       uint64 `json:"id"`
	Checksum string `json:"checksum"`
	Size     uint32 `json:"size"`
	Data     []byte `json:"-"`
}

var blobs = []blob{}
var mutex = sync.Mutex{}
var last_chunk_id = uint64(0)
var file_list = []string{}

func get_file_no_ext(fileName string) (string, error) {
	extensionIndex := strings.LastIndexByte(fileName, '.')
	if extensionIndex != -1 {
		return fileName[:extensionIndex], nil
	}
	return "", errors.New("Unable to parse file name. Does it have an extension?")
}

func (b *blob) generate_blob(output_dir string) {
	data := []byte{}
	for _, chunk := range b.Chunks {
		dataSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataSize, chunk.Size)
		data = append(data, dataSize...)
		data = append(data, chunk.Data...)
	}

	if _, err := os.Stat(output_dir); os.IsNotExist(err) {
		err := os.MkdirAll(output_dir, 0700)
		if err != nil {
			panic(err)
		}
	}

	fileName, err := get_file_no_ext(b.FileName)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(path.Join(output_dir, fileName+".blob"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = f.Write(data)
	defer f.Close()

	if err != nil {
		log.Fatal(err)
	}
}

func (b *blob) generate_chunks(path string, done chan bool) {
	f, err := os.Open(path)

	if err != nil {
		log.Fatal(err)
	}

	for {
		buff := make([]byte, 4096)
		bytesRead, err := f.Read(buff)

		if err != nil {
			break
		}

		h := sha256.New()
		h.Write(buff)

		mutex.Lock()
		last_chunk_id++
		b.Chunks = append(b.Chunks, chunk{
			Id:       last_chunk_id,
			Checksum: fmt.Sprintf("%x", h.Sum(nil)),
			Size:     uint32(bytesRead),
			Data:     buff,
		})
		mutex.Unlock()
	}
	f.Close()
	done <- true
}

func (b *blob) generate_version_sum() {
	string_concat := ""

	for _, c := range b.Chunks {
		string_concat = string_concat + c.Checksum
	}

	mutex.Lock()
	h := sha256.New()
	h.Write([]byte(string_concat))
	b.Version = fmt.Sprintf("%x", h.Sum(nil))
	mutex.Unlock()
}

func walk_directory(path string, dir fs.DirEntry, err error) error {
	if err != nil {
		return err
	}

	if !dir.IsDir() {
		file_list = append(file_list, path)
	}
	return nil
}

func main() {
	done := make(chan bool)
	blob := blob{}

	err := filepath.WalkDir("files", walk_directory)

	if err != nil {
		log.Fatal(err)
	}

	for _, f := range file_list {
		blob.FileName = filepath.Base(f)
		go blob.generate_chunks(f, done)
	}

	for range len(file_list) {
		<-done
		blob.generate_version_sum()
		blobs = append(blobs, blob)
	}

	for _, b := range blobs {
		byteData, err := json.MarshalIndent(blob, "", "\t")
		if err != nil {
			log.Fatal(err)
		}

		err = os.WriteFile("manifest.json", byteData, 0644)
		if err != nil {
			log.Fatal(err)
		}

		b.generate_blob("blob")
	}
}

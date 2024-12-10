package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"unsafe"
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
	Position uint64 `json:"position"`
}

var blobs = []blob{}
var mutex = sync.Mutex{}
var last_chunk_id = uint64(0)
var file_list = []string{}
var stream_position = uint64(0)

func (b *blob) write_to_blob(data *[]byte, output_dir string, output_file string) {
	if _, err := os.Stat(output_dir); os.IsNotExist(err) {
		err := os.MkdirAll(output_dir, 0700)
		if err != nil {
			panic(err)
		}
	}

	f, err := os.OpenFile(path.Join(output_dir, output_file), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	println("Writing blob data...")
	_, err = f.Write(*data)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
}

func (b *blob) append_to_blob(data *[]byte) {
	fmt.Printf("Appending (%s) to blob\n", b.FileName)

	for _, chunk := range b.Chunks {
		dataSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataSize, chunk.Size)
		*data = append(*data, dataSize...)
		*data = append(*data, chunk.Data...)
	}
}

func (b *blob) add_chunks_to_blob(path string) {
	f, err := os.Open(path)

	fmt.Printf("Generate chunks for (%s)\n", path)

	if err != nil {
		log.Fatal(err)
	}

	for {
		buff := make([]byte, 1048576)
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
			Position: stream_position,
		})
		stream_position += uint64(unsafe.Sizeof(uint32(bytesRead)))
		stream_position += uint64(bytesRead)
		mutex.Unlock()
	}
	f.Close()
}

func (b *blob) get_blob_data_version() {
	var stringSize uint64
	for _, c := range b.Chunks {
		stringSize += uint64(len(c.Checksum))
	}

	fmt.Printf("Version string size: %d\n", stringSize)

	string_concat := make([]byte, stringSize)

	for _, c := range b.Chunks {
		string_concat = append(string_concat, []byte(c.Checksum)...)
		if c.Id%100 == 0 {
			fmt.Printf("Concatenating chunk ID (%d)\n", c.Id)
		}
	}

	mutex.Lock()
	h := sha256.New()
	h.Write([]byte(string_concat))
	b.Version = fmt.Sprintf("%x", h.Sum(nil))
	mutex.Unlock()
	fmt.Printf("Blob version: %s\n", b.Version)
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
	err := filepath.WalkDir("files", walk_directory)

	if err != nil {
		log.Fatal(err)
	}

	for _, f := range file_list {
		blob := blob{}
		blob.add_chunks_to_blob(f)
		blob.get_blob_data_version()
		blob.FileName = filepath.Base(f)
		blobs = append(blobs, blob)
	}

	byteData, err := json.MarshalIndent(blobs, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile("manifest.json", byteData, 0644)
	if err != nil {
		log.Fatal(err)
	}

	output_dir := "blob"
	output_file := "data.blob"
	full_path := path.Join(output_dir, output_file)

	_, err = os.Stat(full_path)
	os.IsNotExist(err)
	if err == nil {
		os.Remove(full_path)
	}

	for _, b := range blobs {
		data := []byte{}
		b.append_to_blob(&data)
		b.write_to_blob(&data, output_dir, output_file)
	}
}

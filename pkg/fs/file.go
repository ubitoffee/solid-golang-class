package fs

type File struct {
	SubDir string
	Name   string
	Data   []byte
}

func NewFile(subDir string, name string, data []byte) File {
	return File{
		SubDir: subDir,
		Name:   name,
		Data:   data}
}

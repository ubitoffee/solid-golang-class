package concur

type Job struct {
	Index string
	DocID string
	Body  []byte
}

func NewJob(key string, path string, data []byte) *Job {
	if key == "" || path == "" {
		return nil
	}
	if data == nil {
		return nil
	}
	return &Job{
		Index: key,
		DocID: path,
		Body:  data,
	}
}

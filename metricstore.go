package metricstore

import (
	"github.com/PreetamJinka/listmap"

	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type MetricStore struct {
	baseDir string
}

type MetricPoint struct {
	Timestamp time.Time `json:"ts"`
	Value     float64   `json:"value"`
}

func NewMetricStore(baseDir string) *MetricStore {
	return &MetricStore{
		baseDir: baseDir,
	}
}

func (s *MetricStore) Insert(host, metric string, timestamp time.Time, value float64) error {
	var err error

	path := filepath.Join(s.baseDir, host)
	metricsFile := filepath.Join(path, metric) + ".l"

	err = os.MkdirAll(filepath.Join(s.baseDir, host), 0755)
	if err != nil {
		return err
	}

	var l *listmap.Listmap

	l, err = listmap.OpenListmap(metricsFile)
	if err != nil {
		l, err = listmap.NewListmap(metricsFile)
		if err != nil {
			return err
		}
	}

	err = l.Set(timestampToBytes(timestamp), floatToBytes(value))

	l.Close()

	return err
}

func (s *MetricStore) Retrieve(host, metric string, start, end time.Time) []MetricPoint {
	path := filepath.Join(s.baseDir, host)
	metricsFile := filepath.Join(path, metric) + ".l"

	l, err := listmap.OpenListmap(metricsFile)
	if err != nil {
		return nil
	}

	result := []MetricPoint{}

	for c := l.NewCursor(); c != nil; c = c.Next() {
		ts, val := bytesToTimestamp(c.Key()), bytesToFloat(c.Value())
		if ts.After(start) {
			if ts.Before(end) {
				result = append(result, MetricPoint{
					Timestamp: ts,
					Value:     val,
				})
			} else {
				break
			}
		}
	}

	l.Close()

	return result
}

func (s *MetricStore) Hosts() []string {
	hosts := []string{}
	fileinfos, err := ioutil.ReadDir(s.baseDir)
	if err != nil {
		return hosts
	}

	for _, info := range fileinfos {
		if info.IsDir() {
			hosts = append(hosts, info.Name())
		}
	}

	return hosts
}

func (s *MetricStore) Metrics(host string) []string {
	metrics := []string{}
	fileinfos, err := ioutil.ReadDir(filepath.Join(s.baseDir, host))
	if err != nil {
		return metrics
	}

	for _, info := range fileinfos {
		if !info.IsDir() {
			metrics = append(metrics, strings.TrimSuffix(info.Name(), ".l"))
		}
	}

	return metrics
}

func timestampToBytes(t time.Time) []byte {
	b := &bytes.Buffer{}

	binary.Write(b, binary.BigEndian, uint64(t.Unix()))

	return b.Bytes()
}

func bytesToTimestamp(buf []byte) time.Time {
	b := bytes.NewReader(buf)

	var i uint64
	binary.Read(b, binary.BigEndian, &i)

	return time.Unix(int64(i), 0)
}

func floatToBytes(f float64) []byte {
	b := &bytes.Buffer{}

	binary.Write(b, binary.BigEndian, f)

	return b.Bytes()
}

func bytesToFloat(buf []byte) float64 {
	b := bytes.NewReader(buf)

	var f float64
	binary.Read(b, binary.BigEndian, &f)

	return f
}

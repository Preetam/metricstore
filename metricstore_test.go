package metricstore

import (
	"math/rand"
	"testing"
	"time"
)

func Test1(t *testing.T) {
	s := NewMetricStore("/tmp/metricstore")

	now, start := time.Now(), time.Now()

	for i := 0; i < 1000; i++ {
		s.Insert("host1", "myMetric", now, rand.Float64())
		now = now.Add(time.Second)
	}

	t.Log(s.Retrieve("host1", "myMetric", start, start.Add(time.Minute)))
}

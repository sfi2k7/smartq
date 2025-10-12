package smartq

import (
	"fmt"
	"strconv"
	"time"
)

type Job map[string]string

func (j Job) ID() string {
	return j.String("id")
}

func (j Job) Channel() string {
	return j.String("channel")
}

func (j Job) Created() time.Time {
	return j.Time("created")
}

func (j Job) String(k string) string {
	v, ok := j[k]
	if !ok {
		return ""
	}
	return v
}

func (j Job) Int(k string) int {
	v := j.String(k)
	if v == "" {
		fmt.Println("int: v is empty", k)
		return 0
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		fmt.Println("int: unable to aroi", v, err)
		return 0
	}
	return i
}

func (j Job) Float(k string) float64 {
	v := j.String(k)
	if v == "" {
		return 0.0
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0.0
	}
	return f
}

func (j Job) Bool(k string) bool {
	v := j.String(k)
	if v == "" {
		return false
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

func (j Job) Time(k string) time.Time {
	msecs := j.Int(k)
	if msecs == 0 {
		fmt.Println("time: msecs is 0")
		return time.Time{}
	}

	t := time.Unix(int64(msecs), 0)

	return t
}

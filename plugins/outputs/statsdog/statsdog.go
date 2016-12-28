package statsdog

import (
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/moonfrog/go-logs/logs"
	"github.com/moonfrog/telegraf"
	"github.com/moonfrog/telegraf/internal"
	"github.com/moonfrog/telegraf/plugins/outputs"
	"sort"
	"strings"
	"sync"
	"time"
)

type Statsdog struct {
	Server  string
	Timeout internal.Duration
	client  *statsd.Client
}

var sampleConfig = `
  ## Connection timeout.
  # timeout = "5s"
`

type TimeSeries struct {
	Series []*Metric `json:"series"`
}

type Metric struct {
	Metric string   `json:"metric"`
	Points [1]Point `json:"points"`
	Host   string   `json:"host"`
	Type   string   `json:"type"`
	Tags   []string `json:"tags,omitempty"`
}

func (this *Metric) Line() string {
	tags := ""
	if this.Tags != nil {
		tags = strings.Join(this.Tags, " # ")
	}
	line := fmt.Sprintf("%v | (%v) | %v | (%v) | %v,", this.Metric, this.Points[0].String(), this.Type, tags, this.Host)
	return line
}

func (this *Metric) Lines(x int) []string {
	lines := make([]string, 0)
	if this.Tags == nil {
		return lines
	}
	for _, t := range this.Tags {
		if !strings.HasPrefix(t, "host:") && !strings.HasPrefix(t, "modes") {
			l := fmt.Sprintf("%v : %v, %v, %v=[%+v]", x, this.Metric, this.Type, t, this.Points[0].String())
			lines = append(lines, l)
		}
	}
	return lines
}

type Point [2]float64

func (this *Point) String() string {
	x := time.Unix(int64(this[0]), 0).Format("15:04:05")
	return fmt.Sprintf("%v,%v", x, this[1])
}

func NewStatsdog() *Statsdog {
	return &Statsdog{}
}

func (d *Statsdog) Connect() error {
	var err error

	d.client, err = statsd.New("172.31.22.127:8125")
	//d.client, err = statsd.New("127.0.0.1:9999")
	//d.client, err = statsd.NewBuffered("172.31.22.127:8125", 10)
	if err != nil {
		logs.Fatalf(err.Error())
	}

	return nil
}

func (d *Statsdog) Write(metrics []telegraf.Metric) error {
	//fmt.Printf("+%v", metrics)
	//return nil

	if len(metrics) == 0 {
		return nil
	}
	ts := TimeSeries{}
	tempSeries := []*Metric{}
	metricCounter := 0

	for _, m := range metrics {
		if dogMs, err := buildMetrics(m); err == nil {
			for fieldName, dogM := range dogMs {
				// name of the datadog measurement
				var dname string
				if fieldName == "value" {
					// adding .value seems redundant here
					dname = m.Name()
				} else {
					dname = m.Name() + "." + fieldName
				}
				var host string
				var mode string
				host, _ = m.Tags()["host"]
				mode, _ = m.Tags()["modes"]

				metric := &Metric{
					Metric: dname,
					Tags:   buildTags(m.Tags()),
					Host:   host,
					Type:   mode,
				}
				metric.Points[0] = dogM
				tempSeries = append(tempSeries, metric)
				metricCounter++
			}
		} else {
			logs.Infof("unable to build Metric for %s, skipping", m.Name())
		}
	}

	ts.Series = make([]*Metric, metricCounter)
	copy(ts.Series, tempSeries[0:])

	stime := time.Now().Unix()
	var wg sync.WaitGroup
	wg.Add(1)
	go d.sendWithSleep(ts.Series, &wg)
	wg.Wait()
	logs.Infof("to-dog : time taken : %v", time.Now().Unix()-stime)

	return nil
}

func (d *Statsdog) sendWithSleep(metrics []*Metric, wg *sync.WaitGroup) {

	//printMetrics(ts.Series)
	//fmt.Printf("writing ======= \n")

	for i, m := range metrics {
		switch m.Type {
		case "count":
			value := int64(m.Points[0][1])
			if err := d.client.Count(m.Metric, value, m.Tags, 1); err != nil {
				logs.Errorf(err.Error())
			} else {
				logs.Infof("to-dog : %v ", m.Line())
			}
		case "gauge":
			value := m.Points[0][1]
			if err := d.client.Gauge(m.Metric, value, m.Tags, 1); err != nil {
				logs.Errorf(err.Error())
			} else {
				logs.Infof("to-dog : %v ", m.Line())
			}
		default:
			logs.Errorf("invalid metric-type(%v), metric(%+v)", m.Type, m.Line())
		}
		if i%30 == 0 {
			logs.Infof("to-dog : sleeping")
			time.Sleep(10 * time.Millisecond)
		}
	}
	wg.Done()
}

func printMetrics(metrics []*Metric) {
	if metrics == nil {
		logs.Infof("metric-array-nil")
		return
	}
	lines := make([]string, 0)
	for x, m := range metrics {
		if m != nil {
			lines = append(lines, m.Lines(x)...)
		}
	}

	for _, l := range lines {
		logs.Infof("Metric : %v", l)
	}
}

func (d *Statsdog) SampleConfig() string {
	return sampleConfig
}

func (d *Statsdog) Description() string {
	return "Configuration for DataDog API to send metrics to."
}

func buildMetrics(m telegraf.Metric) (map[string]Point, error) {
	ms := make(map[string]Point)
	for k, v := range m.Fields() {
		if !verifyValue(v) {
			continue
		}
		var p Point
		if err := p.setValue(v); err != nil {
			return ms, fmt.Errorf("unable to extract value from Fields, %s", err.Error())
		}
		p[0] = float64(m.Time().Unix())
		ms[k] = p
	}
	return ms, nil
}

func buildTags(mTags map[string]string) []string {
	tags := make([]string, len(mTags))
	index := 0
	for k, v := range mTags {
		tags[index] = fmt.Sprintf("%s:%s", k, v)
		index += 1
	}
	sort.Strings(tags)
	return tags
}

func verifyValue(v interface{}) bool {
	switch v.(type) {
	case string:
		return false
	}
	return true
}

func (p *Point) setValue(v interface{}) error {
	switch d := v.(type) {
	case int:
		p[1] = float64(int(d))
	case int32:
		p[1] = float64(int32(d))
	case int64:
		p[1] = float64(int64(d))
	case float32:
		p[1] = float64(d)
	case float64:
		p[1] = float64(d)
	default:
		return fmt.Errorf("undeterminable type")
	}
	return nil
}

func (d *Statsdog) Close() error {
	return nil
}

func init() {
	outputs.Add("statsdog", func() telegraf.Output {
		return NewStatsdog()
	})
}

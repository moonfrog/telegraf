package statsdog

import (
	//"bytes"
	//"encoding/json"
	"fmt"
	"log"
	//"net/url"
	"sort"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/moonfrog/telegraf"
	"github.com/moonfrog/telegraf/internal"
	"github.com/moonfrog/telegraf/plugins/outputs"
	"github.com/moonfrog/badger/logs"
	"strings"
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

func (this *Metric) Lines() []string{
	lines := make([]string, 0)
	if this.Tags == nil{
		return lines
	}
	for _, t := range this.Tags{
		if !strings.HasPrefix(t, "host:") && !strings.HasPrefix(t, "modes"){
			l := fmt.Sprintf("%v, %v, %v=[%+v]", this.Metric, this.Type, t, this.Points[0].String())
			lines = append(lines, l)
		}
	}
	return lines
}

type Point [2]float64

func (this *Point) String() string{
	return fmt.Sprintf("%.2f, %v", this[0], this[1] )
}
func NewStatsdog() *Statsdog {
	return &Statsdog{}
}

func (d *Statsdog) Connect() error {
	var err error

	d.client, err = statsd.New("172.31.22.127:8125")
	if err != nil {
		log.Fatal(err)
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
			log.Printf("I! unable to build Metric for %s, skipping\n", m.Name())
		}
	}

	ts.Series = make([]*Metric, metricCounter)
	copy(ts.Series, tempSeries[0:])

	printMetrics(ts.Series)

	for _, m := range ts.Series{
		switch m.Type {
		case "count":
			value := int64(m.Points[0][1])
			d.client.Count(m.Metric,value,m.Tags,1)
		case "gauge":
			value := m.Points[0][1]
			d.client.Gauge(m.Metric,value, m.Tags, 1)
		}
	}

	return nil
}

func printMetrics(metrics []*Metric){
	if metrics == nil{
		logs.Info("metric-array-nil")
		return
	}
	lines := make([]string, 0)
	for _, m := range metrics{
		if m!=nil{
			lines = append(lines, m.Lines()...)
		}
	}

	for _, l := range lines{
		logs.Info("Metric : %v", l)
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

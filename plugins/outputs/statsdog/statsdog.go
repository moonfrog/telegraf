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
	Tags   []string `json:"tags,omitempty"`
}

type Point [2]float64

func NewStatsdog() *Statsdog {
	return &Statsdog{}
}

func (d *Statsdog) Connect() error {
	var err error

	d.client, err = statsd.New("172.31.22.127:8125")
	if err != nil {
		fmt.Println("hello")
		log.Fatal(err)
	}

	return nil
}

func (d *Statsdog) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	// send the EC2 availability zone as a tag with every metric
	d.client.Tags = append(d.client.Tags, "region:us-east-1a")
	err := d.client.Gauge("request.duration", 1.2, nil, 1)
	if err != nil {
		fmt.Println(err)
	}

	/*for _, m := range metrics {
		if dogMs, err := buildMetrics(m); err == nil {
			fmt.Printf("hello -- +%v", dogMs)
		}
	}*/

	/*tags := make([]string, 1)
	tags["meter"] = "test"

	err := d.client.Gauge("opt", 1, tags, 1)
	if err != nil {
		fmt.Println(err)
	}*/

	metric := &Metric{}
	//err = c.Gauge("request.duration", 1.2, nil, 1)
	/*ts := TimeSeries{}

	metricCounter := 0*/

	for _, m := range metrics {
		if dogMs, err := buildMetrics(m); err == nil {
			for fieldName, dogM := range dogMs {
				// name of the datadog measurement
				var dname string
				if fieldName == "value" {
					// adding .value seems redundant here
					dname = m.Name()
				} else if fieldName == "mode" {
					mode := dogM
					fmt.Printf("+%v", mode)
				} else {
					dname = m.Name() + "." + fieldName
				}
				var host string
				host, _ = m.Tags()["host"]
				metric = &Metric{
					Metric: dname,
					Tags:   buildTags(m.Tags()),
					Host:   host,
				}
				metric.Points[0] = dogM

				//tempSeries = append(tempSeries, metric)
				//metricCounter++
			}
		} else {
			log.Printf("I! unable to build Metric for %s, skipping\n", m.Name())
		}
	}

	/*
		ts.Series = make([]*Metric, metricCounter)
		copy(ts.Series, tempSeries[0:])
		tsBytes, err := json.Marshal(ts)
		if err != nil {
			return fmt.Errorf("unable to marshal TimeSeries, %s\n", err.Error())
		}
		req, err := http.NewRequest("POST", d.authenticatedUrl(), bytes.NewBuffer(tsBytes))
		if err != nil {
			return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
		}
		req.Header.Add("Content-Type", "application/json")

		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing metrics, %s\n", err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 209 {
			return fmt.Errorf("received bad status code, %d\n", resp.StatusCode)
		}*/

	return nil
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
		fmt.Print(k)
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

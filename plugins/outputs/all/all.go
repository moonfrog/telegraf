package all

import (
	_ "github.com/moonfrog/telegraf/plugins/outputs/amon"
	_ "github.com/moonfrog/telegraf/plugins/outputs/amqp"
	_ "github.com/moonfrog/telegraf/plugins/outputs/cloudwatch"
	_ "github.com/moonfrog/telegraf/plugins/outputs/datadog"
	_ "github.com/moonfrog/telegraf/plugins/outputs/file"
	_ "github.com/moonfrog/telegraf/plugins/outputs/graphite"
	_ "github.com/moonfrog/telegraf/plugins/outputs/graylog"
	_ "github.com/moonfrog/telegraf/plugins/outputs/influxdb"
	_ "github.com/moonfrog/telegraf/plugins/outputs/instrumental"
	_ "github.com/moonfrog/telegraf/plugins/outputs/kafka"
	_ "github.com/moonfrog/telegraf/plugins/outputs/kinesis"
	_ "github.com/moonfrog/telegraf/plugins/outputs/librato"
	_ "github.com/moonfrog/telegraf/plugins/outputs/mqtt"
	_ "github.com/moonfrog/telegraf/plugins/outputs/nats"
	_ "github.com/moonfrog/telegraf/plugins/outputs/nsq"
	_ "github.com/moonfrog/telegraf/plugins/outputs/opentsdb"
	_ "github.com/moonfrog/telegraf/plugins/outputs/prometheus_client"
	_ "github.com/moonfrog/telegraf/plugins/outputs/riemann"
)

package apache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/moonfrog/telegraf/testutil"

	"github.com/stretchr/testify/require"
)

var apacheStatus = `
Total Accesses: 129811861
Total kBytes: 5213701865
CPULoad: 6.51929
Uptime: 941553
ReqPerSec: 137.87
BytesPerSec: 5670240
BytesPerReq: 41127.4
BusyWorkers: 270
IdleWorkers: 630
ConnsTotal: 1451
ConnsAsyncWriting: 32
ConnsAsyncKeepAlive: 945
ConnsAsyncClosing: 205
Scoreboard: WW_____W_RW_R_W__RRR____WR_W___WW________W_WW_W_____R__R_WR__WRWR_RRRW___R_RWW__WWWRW__R_RW___RR_RW_R__W__WR_WWW______WWR__R___R_WR_W___RW______RR________________W______R__RR______W________________R____R__________________________RW_W____R_____W_R_________________R____RR__W___R_R____RW______R____W______W_W_R_R______R__R_R__________R____W_______WW____W____RR__W_____W_R_______W__________W___W____________W_______WRR_R_W____W_____R____W_WW_R____RRW__W............................................................................................................................................................................................................................................................................................................WRRWR____WR__RR_R___RWR_________W_R____RWRRR____R_R__RW_R___WWW_RW__WR_RRR____W___R____WW_R__R___RR_W_W_RRRRWR__RRWR__RRW_W_RRRW_R_RR_W__RR_RWRR_R__R___RR_RR______R__RR____R_____W_R_R_R__R__R__________W____WW_R___R_R___R_________RR__RR____RWWWW___W_R________R_R____R_W___W___R___W_WRRWW_______R__W_RW_______R________RR__R________W_______________________W_W______________RW_________WR__R___R__R_______________WR_R_________W___RW_____R____________W____......................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................
`

func TestHTTPApache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, apacheStatus)
	}))
	defer ts.Close()

	a := Apache{
		// Fetch it 2 times to catch possible data races.
		Urls: []string{ts.URL, ts.URL},
	}

	var acc testutil.Accumulator
	err := a.Gather(&acc)
	require.NoError(t, err)

	fields := map[string]interface{}{
		"TotalAccesses":        float64(1.29811861e+08),
		"TotalkBytes":          float64(5.213701865e+09),
		"CPULoad":              float64(6.51929),
		"Uptime":               float64(941553),
		"ReqPerSec":            float64(137.87),
		"BytesPerSec":          float64(5.67024e+06),
		"BytesPerReq":          float64(41127.4),
		"BusyWorkers":          float64(270),
		"IdleWorkers":          float64(630),
		"ConnsTotal":           float64(1451),
		"ConnsAsyncWriting":    float64(32),
		"ConnsAsyncKeepAlive":  float64(945),
		"ConnsAsyncClosing":    float64(205),
		"scboard_waiting":      float64(630),
		"scboard_starting":     float64(0),
		"scboard_reading":      float64(157),
		"scboard_sending":      float64(113),
		"scboard_keepalive":    float64(0),
		"scboard_dnslookup":    float64(0),
		"scboard_closing":      float64(0),
		"scboard_logging":      float64(0),
		"scboard_finishing":    float64(0),
		"scboard_idle_cleanup": float64(0),
		"scboard_open":         float64(2850),
	}
	acc.AssertContainsFields(t, "apache", fields)
}

// +build linux

package iptables

import (
	"errors"
	"reflect"
	"testing"

	"github.com/moonfrog/telegraf/testutil"
)

func TestIptables_Gather(t *testing.T) {
	tests := []struct {
		table  string
		chains []string
		values []string
		tags   []map[string]string
		fields [][]map[string]interface{}
		err    error
	}{
		{ // 1 - no configured table => no results
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
		                `},
		},
		{ // 2 - no configured chains => no results
			table: "filter",
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
		                `},
		},
		{ // 3 - pkts and bytes are gathered as integers
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0   /* foobar */
		                `},
			tags: []map[string]string{map[string]string{"table": "filter", "chain": "INPUT", "ruleid": "foobar"}},
			fields: [][]map[string]interface{}{
				{map[string]interface{}{"pkts": uint64(57), "bytes": uint64(4520)}},
			},
		},
		{ // 4 - missing fields header => no results
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)`},
		},
		{ // 5 - invalid chain header => error
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{
				`INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
		                `},
			err: errParse,
		},
		{ // 6 - invalid fields header => error
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)

		                57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
		                `},
			err: errParse,
		},
		{ // 7 - invalid integer value => best effort, no error
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                K     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
            `},
		},
		{ // 8 - Multiple rows, multipe chains => no error
			table:  "filter",
			chains: []string{"INPUT", "FORWARD"},
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                100     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0
		                200     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0  /* foo */
		                `,
				`Chain FORWARD (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
		                300     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0  /* bar */
		                400     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0 
		                500     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0 /* foobar */
		                `,
			},
			tags: []map[string]string{
				map[string]string{"table": "filter", "chain": "INPUT", "ruleid": "foo"},
				map[string]string{"table": "filter", "chain": "FORWARD", "ruleid": "bar"},
				map[string]string{"table": "filter", "chain": "FORWARD", "ruleid": "foobar"},
			},
			fields: [][]map[string]interface{}{
				{map[string]interface{}{"pkts": uint64(200), "bytes": uint64(4520)}},
				{map[string]interface{}{"pkts": uint64(300), "bytes": uint64(4520)}},
				{map[string]interface{}{"pkts": uint64(500), "bytes": uint64(4520)}},
			},
		},
		{ // 9 - comments are used as ruleid if any
			table:  "filter",
			chains: []string{"INPUT"},
			values: []string{
				`Chain INPUT (policy ACCEPT 58 packets, 5096 bytes)
		                pkts      bytes target     prot opt in     out     source               destination
                        57     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0             tcp dpt:22 /* foobar */
                        100     4520 RETURN     tcp  --  *      *       0.0.0.0/0            0.0.0.0/0    tcp dpt:80
		                `},
			tags: []map[string]string{
				map[string]string{"table": "filter", "chain": "INPUT", "ruleid": "foobar"},
			},
			fields: [][]map[string]interface{}{
				{map[string]interface{}{"pkts": uint64(57), "bytes": uint64(4520)}},
			},
		},
	}

	for i, tt := range tests {
		i++
		ipt := &Iptables{
			Table:  tt.table,
			Chains: tt.chains,
			lister: func(table, chain string) (string, error) {
				if len(tt.values) > 0 {
					v := tt.values[0]
					tt.values = tt.values[1:]
					return v, nil
				}
				return "", nil
			},
		}
		acc := new(testutil.Accumulator)
		err := ipt.Gather(acc)
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d: expected error '%#v' got '%#v'", i, tt.err, err)
		}
		if tt.table == "" {
			n := acc.NFields()
			if n != 0 {
				t.Errorf("%d: expected 0 fields if empty table got %d", i, n)
			}
			continue
		}
		if len(tt.chains) == 0 {
			n := acc.NFields()
			if n != 0 {
				t.Errorf("%d: expected 0 fields if empty chains got %d", i, n)
			}
			continue
		}
		if len(tt.tags) == 0 {
			n := acc.NFields()
			if n != 0 {
				t.Errorf("%d: expected 0 values got %d", i, n)
			}
			continue
		}
		n := 0
		for j, tags := range tt.tags {
			for k, fields := range tt.fields[j] {
				if len(acc.Metrics) < n+1 {
					t.Errorf("%d: expected at least %d values got %d", i, n+1, len(acc.Metrics))
					break
				}
				m := acc.Metrics[n]
				if !reflect.DeepEqual(m.Measurement, measurement) {
					t.Errorf("%d %d %d: expected measurement '%#v' got '%#v'\n", i, j, k, measurement, m.Measurement)
				}
				if !reflect.DeepEqual(m.Tags, tags) {
					t.Errorf("%d %d %d: expected tags\n%#v got\n%#v\n", i, j, k, tags, m.Tags)
				}
				if !reflect.DeepEqual(m.Fields, fields) {
					t.Errorf("%d %d %d: expected fields\n%#v got\n%#v\n", i, j, k, fields, m.Fields)
				}
				n++
			}
		}
	}
}

func TestIptables_Gather_listerError(t *testing.T) {
	errFoo := errors.New("error foobar")
	ipt := &Iptables{
		Table:  "nat",
		Chains: []string{"foo", "bar"},
		lister: func(table, chain string) (string, error) {
			return "", errFoo
		},
	}
	acc := new(testutil.Accumulator)
	err := ipt.Gather(acc)
	if !reflect.DeepEqual(err, errFoo) {
		t.Errorf("Expected error %#v got\n%#v\n", errFoo, err)
	}
}

package main

import (
	"fmt"
	"github.com/moonfrog/go-logs/logs"
	"io/ioutil"
	"net"
)

type Config struct {
	host string
	port string
	prot string
}

func (o *Config) addrStr() string {
	return fmt.Sprintf("%v:%v", o.host, o.port)
}

func (o *Config) String() string {
	return fmt.Sprintf("%v@%v:%v", o.prot, o.host, o.port)
}

func main() {

	var err error
	var cfg *Config

	if cfg, err = getConfig(); err != nil {
		logs.Fatalf("error fetching config (%v)", err.Error())
	}

	var conn *net.UDPConn
	if addr, err := net.ResolveUDPAddr(cfg.prot, cfg.addrStr()); err != nil {
		logs.Fatalf("error resolving address (%v)", err.Error())
	} else if conn, err = net.ListenUDP(cfg.prot, addr); err != nil {
		logs.Fatalf("error starting server (%v)", err.Error())
	} else {
		logs.Infof("server started : (%v)", cfg.String())
	}

	for {
		buffer := make([]byte, 1024)
		if msgLen, sourceAddr, err := conn.ReadFromUDP(buffer); err != nil {
			logs.Errorf("error reading data (%v)", err.Error())
		} else {
			msg := fmt.Sprintf("%+v : (%v) : (%v)", msgLen, sourceAddr, string(buffer[:msgLen]))
			logs.Infof(msg)
		}
	}
}

func getConfig() (*Config, error) {
	return &Config{host: "127.0.0.1", port: "9999", prot: "udp"}, nil
}

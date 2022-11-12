package main

import (
	"fmt"
	"github.com/grafana/dskit/ring"
	"github.com/hashicorp/consul/api"
	json "github.com/json-iterator/go"
)

func main() {
	codec := ring.GetCodec()
	cfg := api.DefaultConfig()
	cfg.Address = "10.42.0.8:2280"
	client, err := api.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	kv := client.KV()
	list, _, err := kv.List("", &api.QueryOptions{})
	if err == nil {
		for _, v := range list {
			fmt.Println(v.Key)
			decode, err := codec.Decode(v.Value)
			if err == nil {
				marshal, _ := json.Marshal(decode)
				fmt.Printf("%v\n", string(marshal))
			}
		}
	}
}

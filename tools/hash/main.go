// hash recomputes the farm Fingerprint32 values the manual quotes, to check them
// against the specimen database: the shard routing key, a task_queue_id range_hash,
// and the CHASM workflow archetype ID.
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/dgryski/go-farm"
)

func main() {
	ns := "73ab11db-a831-4574-b804-b43fedd412e7"
	h := farm.Fingerprint32([]byte(ns + "_" + "order-1001"))
	fmt.Printf("shard key fp32=0x%08x -> shard(4)=%d shard(512)=%d\n", h, h%4+1, h%512+1)

	id, _ := hex.DecodeString("73ab11dba8314574b804b43fedd412e76f726465727301") // ns ‖ "orders" ‖ 0x01
	fmt.Printf("task_queue_id range_hash=%d\n", farm.Fingerprint32(id))

	fmt.Printf("archetype_id(workflow.workflow)=%d\n", farm.Fingerprint32([]byte("workflow.workflow")))
}

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/dgryski/go-farm"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func Charge(ctx context.Context, amount int) (string, error) {
	_ = activity.GetInfo(ctx)
	return fmt.Sprintf("ok:%d", amount), nil
}

func OrderWorkflow(ctx workflow.Context, orderID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	var r string
	if err := workflow.ExecuteActivity(ctx, Charge, 42).Get(ctx, &r); err != nil {
		return "", err
	}
	_ = workflow.Sleep(ctx, time.Second)
	return orderID + ":" + r, nil
}

func dump(name string, b []byte) {
	f, _ := os.Create("/tmp/claude-0/-home-user-technical-manual/2e75fb3c-ef63-59c5-b0e6-bbd594a28896/scratchpad/bytes/" + name + ".bin")
	f.Write(b)
	f.Close()
	fmt.Printf("== %s (%d bytes)\n%s\n", name, len(b), hex.Dump(b))
}

func main() {
	os.MkdirAll("/tmp/claude-0/-home-user-technical-manual/2e75fb3c-ef63-59c5-b0e6-bbd594a28896/scratchpad/bytes", 0755)
	// shard examples
	for _, n := range []uint32{4, 512, 16384} {
		ns := "0b2c7a1e-0000-4000-8000-000000000000"
		h := farm.Fingerprint32([]byte(ns + "_" + "order-1001"))
		fmt.Printf("fingerprint32=%08x shards=%d -> shard %d\n", h, n, h%n+1)
	}
	// payload examples
	dc := converter.GetDefaultDataConverter()
	for _, v := range []interface{}{"order-1001", 42, nil, []byte{0xde, 0xad}, &commonpb.WorkflowExecution{WorkflowId: "w", RunId: "r"}} {
		p, err := dc.ToPayload(v)
		if err != nil {
			panic(err)
		}
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		dump(fmt.Sprintf("payload_%T", v), b)
	}
	z := converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true})
	p, _ := dc.ToPayload("order-1001")
	zp, _ := z.Encode([]*commonpb.Payload{p})
	b, _ := proto.Marshal(zp[0])
	dump("payload_zlib", b)

	nc, err := client.NewNamespaceClient(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		panic(err)
	}
	ret := durationpb.New(72 * time.Hour)
	if err := nc.Register(context.Background(), &workflowservice.RegisterNamespaceRequest{Namespace: "default", WorkflowExecutionRetentionPeriod: ret}); err != nil {
		fmt.Println("register:", err)
	}
	time.Sleep(12 * time.Second)
	var captured = map[string][]byte{}
	icpt := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m, ok := req.(proto.Message); ok {
			bb, _ := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if _, seen := captured[method]; !seen {
				captured[method] = bb
			}
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		if m, ok := reply.(proto.Message); ok {
			bb, _ := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if _, seen := captured[method+"#resp"]; !seen {
				captured[method+"#resp"] = bb
			}
		}
		return err
	}
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233", Namespace: "default",
		ConnectionOptions: client.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(icpt)}}})
	if err != nil {
		panic(err)
	}
	w := worker.New(c, "orders", worker.Options{})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivity(Charge)
	if err := w.Start(); err != nil {
		panic(err)
	}
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: "order-1001", TaskQueue: "orders"}, OrderWorkflow, "order-1001")
	if err != nil {
		panic(err)
	}
	var res string
	if err := run.Get(context.Background(), &res); err != nil {
		panic(err)
	}
	fmt.Println("RESULT", res, "RUNID", run.GetRunID())
	w.Stop()
	resp, err := c.WorkflowService().GetWorkflowExecutionHistory(context.Background(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: "default", Execution: &commonpb.WorkflowExecution{WorkflowId: "order-1001", RunId: run.GetRunID()}})
	if err != nil {
		panic(err)
	}
	for _, e := range resp.History.Events {
		bb, _ := proto.MarshalOptions{Deterministic: true}.Marshal(e)
		dump(fmt.Sprintf("event_%02d_%s", e.EventId, e.EventType.String()), bb)
	}
	hb, _ := proto.Marshal(resp.History)
	dump("history_full", hb)
	for k, v := range captured {
		name := k
		for i := range name {
			if name[i] == '/' || name[i] == '.' || name[i] == '#' {
				name = name[:i] + "_" + name[i+1:]
			}
		}
		dump("rpc"+name, v)
	}
	desc, _ := c.DescribeWorkflowExecution(context.Background(), "order-1001", run.GetRunID())
	fmt.Println("DESC", desc.WorkflowExecutionInfo.Status, desc.WorkflowExecutionInfo.HistoryLength, desc.WorkflowExecutionInfo.HistorySizeBytes)
	c.Close()
}

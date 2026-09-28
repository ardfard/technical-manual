# Temporal Engineering Manual

`temporal-technical-manual.html` is a single self-contained HTML reference to Temporal's
data plane at the byte level: history events, payload encodings, persistence layout,
shard and task-queue keys, and the gRPC surface. Open it in any browser; the only
external resource is Google Fonts (it falls back to system fonts offline).

Every claim links to source at pinned commits:

| repo | commit | version |
|---|---|---|
| temporalio/temporal | `d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d` | server 1.33.0 |
| temporalio/api | `1c27468c756fc8abc800235c33b74c6eba638c88` | go bindings v1.63.5 |
| temporalio/sdk-go | `626130f1fd9de50cfd90b884a3fb796504f22dc1` | 1.49.0 |

## Where the bytes come from

The hex dumps are real. A server built from the pinned commit ran on a file-backed
SQLite store with 4 history shards. One workflow (`OrderWorkflow`: one activity, one
1 s timer, 16 events) was executed, gRPC bodies were captured with a client
interceptor, and blobs were read straight out of the database file.

| path | purpose |
|---|---|
| `tools/probe/main.go` | worker + client that runs the specimen and dumps RPC bodies (build inside the sdk-go module) |
| `tools/annot/main.go` | descriptor-driven protobuf annotator: bytes → field spans (build inside the server module) |
| `tools/desc/main.go` | dumps field/enum tables from compiled descriptors → `data/structs.json` |
| `tools/hash/main.go` | recomputes the farm Fingerprint32 values quoted in the manual |
| `data/annot.json` | annotated specimen bytes (history_node, executions, shards, task queues, timers, RPCs) |
| `data/event_types.json` | EventType ↔ oneof arm mapping extracted from the pinned protos |
| `tools/content/*.html` | manual prose and diagrams |
| `tools/build.py` | assembles the final HTML (`python3 tools/build.py`) |

Appendix F of the manual has the full reproduction steps.

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"unicode/utf8"

	_ "go.temporal.io/api/history/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	_ "go.temporal.io/server/api/persistence/v1"
	_ "go.temporal.io/server/api/token/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type Node struct {
	Off      int     `json:"o"`   // tag start
	TagLen   int     `json:"tl"`  // tag bytes
	LenLen   int     `json:"ll"`  // length-prefix bytes (LEN wire type)
	End      int     `json:"e"`   // end offset (exclusive)
	Num      int32   `json:"n"`
	Wire     int     `json:"w"`
	Name     string  `json:"f"`
	Type     string  `json:"t"`
	Val      string  `json:"v,omitempty"`
	Kids     []*Node `json:"k,omitempty"`
}

func walk(b []byte, base int, md protoreflect.MessageDescriptor) []*Node {
	var out []*Node
	i := 0
	for i < len(b) {
		num, typ, tl := protowire.ConsumeTag(b[i:])
		if tl < 0 {
			return out
		}
		n := &Node{Off: base + i, TagLen: tl, Num: int32(num), Wire: int(typ)}
		var fd protoreflect.FieldDescriptor
		if md != nil {
			fd = md.Fields().ByNumber(num)
		}
		if fd != nil {
			n.Name = string(fd.Name())
			n.Type = fd.Kind().String()
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				n.Type = string(fd.Message().FullName())
			}
			if fd.Kind() == protoreflect.EnumKind {
				n.Type = string(fd.Enum().FullName())
			}
		} else {
			n.Name = "?" + strconv.Itoa(int(num))
		}
		j := i + tl
		switch typ {
		case protowire.VarintType:
			v, l := protowire.ConsumeVarint(b[j:])
			n.End = base + j + l
			if fd != nil && fd.Kind() == protoreflect.EnumKind {
				ev := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(v))
				if ev != nil {
					n.Val = fmt.Sprintf("%d (%s)", v, ev.Name())
				} else {
					n.Val = fmt.Sprint(v)
				}
			} else if fd != nil && (fd.Kind() == protoreflect.Sint64Kind || fd.Kind() == protoreflect.Sint32Kind) {
				n.Val = fmt.Sprint(protowire.DecodeZigZag(v))
			} else if fd != nil && fd.Kind() == protoreflect.BoolKind {
				n.Val = fmt.Sprint(v != 0)
			} else if fd != nil && (fd.Kind() == protoreflect.Int64Kind || fd.Kind() == protoreflect.Int32Kind) {
				n.Val = fmt.Sprint(int64(v))
			} else {
				n.Val = fmt.Sprint(v)
			}
			j += l
		case protowire.Fixed64Type:
			v, l := protowire.ConsumeFixed64(b[j:])
			if fd != nil && fd.Kind() == protoreflect.DoubleKind {
				n.Val = fmt.Sprint(math.Float64frombits(v))
			} else {
				n.Val = fmt.Sprint(v)
			}
			j += l
			n.End = base + j
		case protowire.Fixed32Type:
			v, l := protowire.ConsumeFixed32(b[j:])
			n.Val = fmt.Sprint(v)
			j += l
			n.End = base + j
		case protowire.BytesType:
			ln, l := protowire.ConsumeVarint(b[j:])
			n.LenLen = l
			j += l
			body := b[j : j+int(ln)]
			if fd != nil && fd.Kind() == protoreflect.MessageKind {
				sub := fd.Message()
				if fd.IsMap() {
					sub = fd.Message()
				}
				n.Kids = walk(body, base+j, sub)
				n.Val = fmt.Sprintf("{%d B}", ln)
			} else if fd != nil && fd.Kind() == protoreflect.StringKind {
				n.Val = strconv.Quote(string(body))
			} else if fd != nil && fd.Kind() == protoreflect.BytesKind {
				if utf8.Valid(body) && len(body) > 0 {
					n.Val = strconv.Quote(string(body))
				} else {
					n.Val = fmt.Sprintf("0x%x", body)
				}
			} else if fd != nil && fd.IsList() {
				n.Val = fmt.Sprintf("packed %x", body)
			} else {
				n.Val = fmt.Sprintf("0x%x", body)
			}
			j += int(ln)
			n.End = base + j
		default:
			return out
		}
		out = append(out, n)
		i = j
	}
	return out
}

func main() {
	name, path := os.Args[1], os.Args[2]
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
	if err != nil {
		panic(err)
	}
	res := map[string]interface{}{"msg": name, "hex": fmt.Sprintf("%x", b), "tree": walk(b, 0, mt.Descriptor())}
	json.NewEncoder(os.Stdout).Encode(res)
}

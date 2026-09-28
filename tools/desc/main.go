// desc dumps field and enum tables from the compiled protobuf descriptors
// linked into the Temporal server module, as JSON, for the manual's appendix.
package main

import (
	"encoding/json"
	"os"

	_ "go.temporal.io/api/history/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	_ "go.temporal.io/server/api/enums/v1"
	_ "go.temporal.io/server/api/persistence/v1"
	_ "go.temporal.io/server/api/token/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type F struct {
	N     int32  `json:"n"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Label string `json:"label"`
	Oneof string `json:"oneof,omitempty"`
	Depr  bool   `json:"deprecated,omitempty"`
}
type M struct {
	Name     string  `json:"name"`
	File     string  `json:"file"`
	Fields   []F     `json:"fields"`
	Reserved [][2]int32 `json:"reserved,omitempty"`
}
type EV struct {
	N    int32  `json:"n"`
	Name string `json:"name"`
}
type E struct {
	Name   string `json:"name"`
	File   string `json:"file"`
	Values []EV   `json:"values"`
}

func main() {
	out := map[string]interface{}{}
	var ms []M
	var es []E
	for _, name := range os.Args[1:] {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			panic(name + ": " + err.Error())
		}
		switch x := d.(type) {
		case protoreflect.MessageDescriptor:
			m := M{Name: string(x.FullName()), File: x.ParentFile().Path()}
			for i := 0; i < x.Fields().Len(); i++ {
				f := x.Fields().Get(i)
				t := f.Kind().String()
				if f.Message() != nil {
					t = string(f.Message().FullName())
					if f.IsMap() {
						t = "map<" + f.MapKey().Kind().String() + "," + typeName(f.MapValue()) + ">"
					}
				}
				if f.Enum() != nil {
					t = string(f.Enum().FullName())
				}
				lab := ""
				if f.IsList() {
					lab = "repeated"
				}
				of := ""
				if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
					of = string(o.Name())
				}
				opts := f.Options()
				dep := false
				if fo, ok := opts.(interface{ GetDeprecated() bool }); ok {
					dep = fo.GetDeprecated()
				}
				m.Fields = append(m.Fields, F{N: int32(f.Number()), Name: string(f.Name()), Type: t, Label: lab, Oneof: of, Depr: dep})
			}
			rr := x.ReservedRanges()
			for i := 0; i < rr.Len(); i++ {
				r := rr.Get(i)
				m.Reserved = append(m.Reserved, [2]int32{int32(r[0]), int32(r[1]) - 1})
			}
			ms = append(ms, m)
		case protoreflect.EnumDescriptor:
			e := E{Name: string(x.FullName()), File: x.ParentFile().Path()}
			for i := 0; i < x.Values().Len(); i++ {
				v := x.Values().Get(i)
				e.Values = append(e.Values, EV{N: int32(v.Number()), Name: string(v.Name())})
			}
			es = append(es, e)
		}
	}
	out["messages"] = ms
	out["enums"] = es
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	enc.Encode(out)
}

func typeName(f protoreflect.FieldDescriptor) string {
	if f.Message() != nil {
		return string(f.Message().FullName())
	}
	if f.Enum() != nil {
		return string(f.Enum().FullName())
	}
	return f.Kind().String()
}

package protocol

import (
	"bytes"
	"testing"
)

func TestRESP3ParseAndEncode(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		want   Value
		encode Value
	}{
		{
			name:   "parse boolean true",
			raw:    "#t\r\n",
			want:   Boolean{Value: true},
			encode: Boolean{Value: true},
		},
		{
			name:   "parse boolean false",
			raw:    "#f\r\n",
			want:   Boolean{Value: false},
			encode: Boolean{Value: false},
		},
		{
			name:   "parse null",
			raw:    "_\r\n",
			want:   Null{},
			encode: Null{},
		},
		{
			name:   "parse nested array with resp3 values",
			raw:    "*3\r\n$3\r\nCMD\r\n#t\r\n_\r\n",
			want:   Array{Elements: []Value{BulkString{Data: []byte("CMD")}, Boolean{Value: true}, Null{}}},
			encode: Array{Elements: []Value{BulkString{Data: []byte("CMD")}, Boolean{Value: true}, Null{}}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(bytes.NewReader([]byte(tt.raw)))
			got, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			assertRESPValueEqual(t, got, tt.want)

			raw, err := Encode(tt.encode)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			roundTrip, err := NewParser(bytes.NewReader(raw)).Parse()
			if err != nil {
				t.Fatalf("round-trip Parse() error = %v", err)
			}
			assertRESPValueEqual(t, roundTrip, tt.encode)
		})
	}
}

func TestRESP3ParseRejectsInvalidBoolean(t *testing.T) {
	_, err := NewParser(bytes.NewReader([]byte("#x\r\n"))).Parse()
	if err == nil {
		t.Fatal("Parse() error = nil, want invalid boolean error")
	}
}

func TestBytesCoerceRESP3Values(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		want    string
		wantErr bool
	}{
		{name: "boolean true", value: Boolean{Value: true}, want: "1"},
		{name: "boolean false", value: Boolean{Value: false}, want: "0"},
		{name: "null errors", value: Null{}, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := Bytes(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Bytes() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Bytes() error = %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("Bytes() = %q, want %q", string(got), tt.want)
			}
		})
	}
}

func assertRESPValueEqual(t *testing.T, got Value, want Value) {
	t.Helper()

	switch typedWant := want.(type) {
	case BulkString:
		typedGot, ok := got.(BulkString)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null || string(typedGot.Data) != string(typedWant.Data) {
			t.Fatalf("bulk string = %#v, want %#v", typedGot, typedWant)
		}
	case Boolean:
		typedGot, ok := got.(Boolean)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("boolean = %v, want %v", typedGot.Value, typedWant.Value)
		}
	case Null:
		if _, ok := got.(Null); !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
	case Array:
		typedGot, ok := got.(Array)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("array null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if len(typedGot.Elements) != len(typedWant.Elements) {
			t.Fatalf("len(array.Elements) = %d, want %d", len(typedGot.Elements), len(typedWant.Elements))
		}
		for i := range typedWant.Elements {
			assertRESPValueEqual(t, typedGot.Elements[i], typedWant.Elements[i])
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

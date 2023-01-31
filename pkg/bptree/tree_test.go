package bptree

import (
	"reflect"
	"testing"
)

func TestTree_Scan(t *testing.T) {
	type fields struct {
		order int
		inserts int
	}
	type args struct {
		start []byte
		end []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [][]byte
	}{
		{
			"Insert 5, scan middle 3",
			fields{8, 5},
			args{byteArrayFromOrd(1), byteArrayFromOrd(3),},
			byteArraySliceFromRange(1, 3),
		},
		{
			"Insert 200, scan middle first 100",
			fields{8, 200},
			args{byteArrayFromOrd(0), byteArrayFromOrd(110),},
			byteArraySliceFromRange(0, 110),
		},
		{
			"Insert 1K, scan all",
			fields{8, 1000},
			args{byteArrayFromOrd(0), byteArrayFromOrd(1000),},
			byteArraySliceFromRange(0, 1000),
		},
		{
			"Insert 100, scan overflow",
			fields{8, 100},
			args{byteArrayFromOrd(50), byteArrayFromOrd(1000),},
			byteArraySliceFromRange(50, 100),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tree := NewTree(int(tt.fields.order))
			for i := 0; i <= tt.fields.inserts; i++ {
				buf := byteArrayFromOrd(i)
				tree.Set(buf, buf)
			}
			got := tree.Scan(tt.args.start, tt.args.end)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Node.split() rightChild = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTree_Get(t *testing.T) {
	tests := []struct {
		name   string
		order int
		want   [][]byte
	}{
		{
			"Order 8, Insert 5, Get All",
			8,
			byteArraySliceFromRange(0, 4),
		},
		{
			"Order 8, Insert 100, Get All",
			8,
			byteArraySliceFromRange(0, 99),
		},
		{
			"Order 8, Insert 1000, Get All",
			8,
			byteArraySliceFromRange(0, 999),
		},
		{
			"Order 3, Insert 5, Get All",
			3,
			byteArraySliceFromRange(0, 4),
		},
		{
			"Order 3, Insert 100, Get All",
			3,
			byteArraySliceFromRange(0, 99),
		},
		{
			"Order 3, Insert 1000, Get All",
			3,
			byteArraySliceFromRange(0, 999),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tree := NewTree(tt.order)
			for _, kv := range tt.want {
				tree.Set(kv, kv)
			}
			for _, kv := range tt.want {
				if _, found := tree.Get(kv); !found {
					t.Error("Not found")
				}
			}
		})
	}
}

func TestTree_Delete(t *testing.T) {
	type fields struct {
		order int
		inserts int
	}
	tests := []struct {
		name   string
		fields fields
		reverse   bool
	}{
		{
			"Insert 1000, remove one by one in order",
			fields{4, 1000},
			false,
		},
		{
			"Insert 100, remove one by one in reverse order",
			fields{4, 100},
			true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tree := NewTree(int(tt.fields.order))
			for i := 0; i < tt.fields.inserts; i++ {
				buf := byteArrayFromOrd(i)
				tree.Set(buf, buf)
			}
			if tt.reverse {
				for i := tt.fields.inserts - 1; i >= 0; i-- {
					buf := byteArrayFromOrd(i)
					if _, found := tree.Get(buf); !found {
						t.Error("Key not found before delete.")
					}
					tree.Delete(buf)
					if _, found := tree.Get(buf); found {
						t.Error("Key found after delete.")
					}
				}
			} else {
				for i:= 0; i < tt.fields.inserts - 1; i++ {
					buf := byteArrayFromOrd(i)
					if _, found := tree.Get(buf); !found {
						t.Error("Key not found before delete.")
					}
					tree.Delete(buf)
					if _, found := tree.Get(buf); found {
						t.Error("Key found after delete.")
					}
				}
			}
			_, ok := tree.root.(*leaf)
			if !ok {
				t.Error("Tree not shrinked properly")
			}
		})
	}
}

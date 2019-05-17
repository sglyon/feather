// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package fbs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type CTable struct {
	_tab flatbuffers.Table
}

func GetRootAsCTable(buf []byte, offset flatbuffers.UOffsetT) *CTable {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CTable{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *CTable) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CTable) Table() flatbuffers.Table {
	return rcv._tab
}

/// Some text (or a name) metadata about what the file is, optional
func (rcv *CTable) Description() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

/// Some text (or a name) metadata about what the file is, optional
func (rcv *CTable) NumRows() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *CTable) MutateNumRows(n int64) bool {
	return rcv._tab.MutateInt64Slot(6, n)
}

func (rcv *CTable) Columns(obj *Column, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *CTable) ColumnsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// Version number of the Feather format
func (rcv *CTable) Version() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

/// Version number of the Feather format
func (rcv *CTable) MutateVersion(n int32) bool {
	return rcv._tab.MutateInt32Slot(10, n)
}

/// Table metadata (likely JSON), not yet used
func (rcv *CTable) Metadata() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

/// Table metadata (likely JSON), not yet used
func CTableStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func CTableAddDescription(builder *flatbuffers.Builder, description flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(description), 0)
}
func CTableAddNumRows(builder *flatbuffers.Builder, numRows int64) {
	builder.PrependInt64Slot(1, numRows, 0)
}
func CTableAddColumns(builder *flatbuffers.Builder, columns flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(columns), 0)
}
func CTableStartColumnsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func CTableAddVersion(builder *flatbuffers.Builder, version int32) {
	builder.PrependInt32Slot(3, version, 0)
}
func CTableAddMetadata(builder *flatbuffers.Builder, metadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(metadata), 0)
}
func CTableEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

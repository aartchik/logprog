package log_v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Record struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Value         []byte                 `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Offset        uint64                 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	Term          uint64                 `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
	Type          uint32                 `protobuf:"varint,4,opt,name=type,proto3" json:"type,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Record) Reset() {
	*x = Record{}
	mi := &file_api_v1_log_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Record) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Record) ProtoMessage() {}

func (x *Record) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Record) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{0}
}

func (x *Record) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Record) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *Record) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *Record) GetType() uint32 {
	if x != nil {
		return x.Type
	}
	return 0
}

type ProduceRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Record        *Record                `protobuf:"bytes,1,opt,name=record,proto3" json:"record,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ProduceRequest) Reset() {
	*x = ProduceRequest{}
	mi := &file_api_v1_log_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProduceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProduceRequest) ProtoMessage() {}

func (x *ProduceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ProduceRequest) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{1}
}

func (x *ProduceRequest) GetRecord() *Record {
	if x != nil {
		return x.Record
	}
	return nil
}

type ProduceResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Offset        uint64                 `protobuf:"varint,1,opt,name=offset,proto3" json:"offset,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ProduceResponse) Reset() {
	*x = ProduceResponse{}
	mi := &file_api_v1_log_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProduceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProduceResponse) ProtoMessage() {}

func (x *ProduceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ProduceResponse) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{2}
}

func (x *ProduceResponse) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

type ConsumeRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Offset        uint64                 `protobuf:"varint,1,opt,name=offset,proto3" json:"offset,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConsumeRequest) Reset() {
	*x = ConsumeRequest{}
	mi := &file_api_v1_log_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConsumeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConsumeRequest) ProtoMessage() {}

func (x *ConsumeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ConsumeRequest) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{3}
}

func (x *ConsumeRequest) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

type ConsumeResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Record        *Record                `protobuf:"bytes,2,opt,name=record,proto3" json:"record,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConsumeResponse) Reset() {
	*x = ConsumeResponse{}
	mi := &file_api_v1_log_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConsumeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConsumeResponse) ProtoMessage() {}

func (x *ConsumeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ConsumeResponse) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{4}
}

func (x *ConsumeResponse) GetRecord() *Record {
	if x != nil {
		return x.Record
	}
	return nil
}

type GetServersRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetServersRequest) Reset() {
	*x = GetServersRequest{}
	mi := &file_api_v1_log_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetServersRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServersRequest) ProtoMessage() {}

func (x *GetServersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetServersRequest) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{5}
}

type GetServersResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Servers       []*Server              `protobuf:"bytes,1,rep,name=servers,proto3" json:"servers,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetServersResponse) Reset() {
	*x = GetServersResponse{}
	mi := &file_api_v1_log_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetServersResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetServersResponse) ProtoMessage() {}

func (x *GetServersResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetServersResponse) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{6}
}

func (x *GetServersResponse) GetServers() []*Server {
	if x != nil {
		return x.Servers
	}
	return nil
}

type Server struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	RpcAddr       string                 `protobuf:"bytes,2,opt,name=rpc_addr,json=rpcAddr,proto3" json:"rpc_addr,omitempty"`
	IsLeader      bool                   `protobuf:"varint,3,opt,name=is_leader,json=isLeader,proto3" json:"is_leader,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Server) Reset() {
	*x = Server{}
	mi := &file_api_v1_log_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Server) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Server) ProtoMessage() {}

func (x *Server) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_log_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Server) Descriptor() ([]byte, []int) {
	return file_api_v1_log_proto_rawDescGZIP(), []int{7}
}

func (x *Server) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Server) GetRpcAddr() string {
	if x != nil {
		return x.RpcAddr
	}
	return ""
}

func (x *Server) GetIsLeader() bool {
	if x != nil {
		return x.IsLeader
	}
	return false
}

var File_api_v1_log_proto protoreflect.FileDescriptor

const file_api_v1_log_proto_rawDesc = "" +
	"\n" +
	"\x10api/v1/log.proto\x12\x06log.v1\"^\n" +
	"\x06Record\x12\x14\n" +
	"\x05value\x18\x01 \x01(\fR\x05value\x12\x16\n" +
	"\x06offset\x18\x02 \x01(\x04R\x06offset\x12\x12\n" +
	"\x04term\x18\x03 \x01(\x04R\x04term\x12\x12\n" +
	"\x04type\x18\x04 \x01(\rR\x04type\"8\n" +
	"\x0eProduceRequest\x12&\n" +
	"\x06record\x18\x01 \x01(\v2\x0e.log.v1.RecordR\x06record\")\n" +
	"\x0fProduceResponse\x12\x16\n" +
	"\x06offset\x18\x01 \x01(\x04R\x06offset\"(\n" +
	"\x0eConsumeRequest\x12\x16\n" +
	"\x06offset\x18\x01 \x01(\x04R\x06offset\"9\n" +
	"\x0fConsumeResponse\x12&\n" +
	"\x06record\x18\x02 \x01(\v2\x0e.log.v1.RecordR\x06record\"\x13\n" +
	"\x11GetServersRequest\">\n" +
	"\x12GetServersResponse\x12(\n" +
	"\aservers\x18\x01 \x03(\v2\x0e.log.v1.ServerR\aservers\"P\n" +
	"\x06Server\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\tR\x02id\x12\x19\n" +
	"\brpc_addr\x18\x02 \x01(\tR\arpcAddr\x12\x1b\n" +
	"\tis_leader\x18\x03 \x01(\bR\bisLeader2\xd6\x02\n" +
	"\x03Log\x12<\n" +
	"\aProduce\x12\x16.log.v1.ProduceRequest\x1a\x17.log.v1.ProduceResponse\"\x00\x12<\n" +
	"\aConsume\x12\x16.log.v1.ConsumeRequest\x1a\x17.log.v1.ConsumeResponse\"\x00\x12D\n" +
	"\rConsumeStream\x12\x16.log.v1.ConsumeRequest\x1a\x17.log.v1.ConsumeResponse\"\x000\x01\x12F\n" +
	"\rProduceStream\x12\x16.log.v1.ProduceRequest\x1a\x17.log.v1.ProduceResponse\"\x00(\x010\x01\x12E\n" +
	"\n" +
	"GetServers\x12\x19.log.v1.GetServersRequest\x1a\x1a.log.v1.GetServersResponse\"\x00B\x14Z\x12logprog/api/log_v1b\x06proto3"

var (
	file_api_v1_log_proto_rawDescOnce sync.Once
	file_api_v1_log_proto_rawDescData []byte
)

func file_api_v1_log_proto_rawDescGZIP() []byte {
	file_api_v1_log_proto_rawDescOnce.Do(func() {
		file_api_v1_log_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_api_v1_log_proto_rawDesc), len(file_api_v1_log_proto_rawDesc)))
	})
	return file_api_v1_log_proto_rawDescData
}

var file_api_v1_log_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_api_v1_log_proto_goTypes = []any{
	(*Record)(nil),
	(*ProduceRequest)(nil),
	(*ProduceResponse)(nil),
	(*ConsumeRequest)(nil),
	(*ConsumeResponse)(nil),
	(*GetServersRequest)(nil),
	(*GetServersResponse)(nil),
	(*Server)(nil),
}
var file_api_v1_log_proto_depIdxs = []int32{
	0,
	0,
	7,
	1,
	3,
	3,
	1,
	5,
	2,
	4,
	4,
	2,
	6,
	8,
	3,
	3,
	3,
	0,
}

func init() { file_api_v1_log_proto_init() }
func file_api_v1_log_proto_init() {
	if File_api_v1_log_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_api_v1_log_proto_rawDesc), len(file_api_v1_log_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_v1_log_proto_goTypes,
		DependencyIndexes: file_api_v1_log_proto_depIdxs,
		MessageInfos:      file_api_v1_log_proto_msgTypes,
	}.Build()
	File_api_v1_log_proto = out.File
	file_api_v1_log_proto_goTypes = nil
	file_api_v1_log_proto_depIdxs = nil
}

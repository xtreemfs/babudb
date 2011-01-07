PROTOC=/usr/bin/protoc
PROTOBUF_INCLUDE=/usr/include

PBRPC_PLUGIN=share/protoc-gen-pbrpc

PROTO_FILES=$(wildcard interface/*.proto)

TARGET_DIR_JAVA_BABUDB=src/
GENERATED_DIR_JAVA_BABUDB=$(TARGET_DIR_JAVA_FOUNDATION)org/xtreemfs/babudb/pbrpc/

clean:
	@rm -rf $(GENERATED_DIR_JAVA_SERVERS)*.java

protocol:
	$(PROTOC) -I$(PROTOBUF_INCLUDE) -I. -I$(@D)/interface/ $(PROTO_FILES) --java_out=$(TARGET_DIR_JAVA_BABUDB)
	$(PROTOC) --plugin=$(PBRPC_PLUGIN) -I$(PROTOBUF_INCLUDE) -I. -I$(@D)/interface/ $(PROTO_FILES) --pbrpc_out=$(TARGET_DIR_JAVA_BABUDB)


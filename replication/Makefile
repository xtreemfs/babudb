PROTOC=/usr/bin/protoc

PBRPC_PLUGIN=share/protoc-gen-pbrpc

PROTO_FILES=$(wildcard interface/*.proto)
FOUNDATION_INCLUDE=$(wildcard share/foundation/include/*.proto)

TARGET_DIR_JAVA_BABUDB=src/
GENERATED_DIR_JAVA_BABUDB=$(TARGET_DIR_JAVA_BABUDB)org/xtreemfs/babudb/pbrpc/

clean:
	@rm -rf $(GENERATED_DIR_JAVA_BABUDB)*.java
	@rm -rf $(TARGET_DIR_JAVA_BABUDB)/com/
	@echo "done"
protocol:
	@$(PROTOC) --proto_path=. --java_out=$(TARGET_DIR_JAVA_BABUDB) share/google/protobuf/descriptor.proto $(FOUNDATION_INCLUDE) $(PROTO_FILES)
	@$(PROTOC) --proto_path=. --plugin=$(PBRPC_PLUGIN) --pbrpc_out=$(TARGET_DIR_JAVA_BABUDB) $(PROTO_FILES)
	@echo "done"

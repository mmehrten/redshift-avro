SRC_DIR=$(pwd)
DST_DIR=$(pwd)
protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/aggregated_record.proto

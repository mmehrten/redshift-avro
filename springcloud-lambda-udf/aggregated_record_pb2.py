# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: aggregated_record.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x17\x61ggregated_record.proto\"!\n\x03Tag\x12\x0b\n\x03key\x18\x01 \x02(\t\x12\r\n\x05value\x18\x02 \x01(\t\"h\n\x06Record\x12\x1b\n\x13partition_key_index\x18\x01 \x02(\x04\x12\x1f\n\x17\x65xplicit_hash_key_index\x18\x02 \x01(\x04\x12\x0c\n\x04\x64\x61ta\x18\x03 \x02(\x0c\x12\x12\n\x04tags\x18\x04 \x03(\x0b\x32\x04.Tag\"j\n\x10\x41ggregatedRecord\x12\x1b\n\x13partition_key_table\x18\x01 \x03(\t\x12\x1f\n\x17\x65xplicit_hash_key_table\x18\x02 \x03(\t\x12\x18\n\x07records\x18\x03 \x03(\x0b\x32\x07.Record')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'aggregated_record_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _TAG._serialized_start=27
  _TAG._serialized_end=60
  _RECORD._serialized_start=62
  _RECORD._serialized_end=166
  _AGGREGATEDRECORD._serialized_start=168
  _AGGREGATEDRECORD._serialized_end=274
# @@protoc_insertion_point(module_scope)

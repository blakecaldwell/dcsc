# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='fib.proto',
  package='fib',
  serialized_pb='\n\tfib.proto\x12\x03\x66ib\"\"\n\x03\x46ib\x12\t\n\x01n\x18\x01 \x02(\x05\x12\x10\n\x08response\x18\x02 \x01(\t\"!\n\x07\x46ibList\x12\x16\n\x04\x66ibs\x18\x01 \x03(\x0b\x32\x08.fib.Fib')




_FIB = descriptor.Descriptor(
  name='Fib',
  full_name='fib.Fib',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='n', full_name='fib.Fib.n', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='response', full_name='fib.Fib.response', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=18,
  serialized_end=52,
)


_FIBLIST = descriptor.Descriptor(
  name='FibList',
  full_name='fib.FibList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='fibs', full_name='fib.FibList.fibs', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=54,
  serialized_end=87,
)

_FIBLIST.fields_by_name['fibs'].message_type = _FIB
DESCRIPTOR.message_types_by_name['Fib'] = _FIB
DESCRIPTOR.message_types_by_name['FibList'] = _FIBLIST

class Fib(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _FIB
  
  # @@protoc_insertion_point(class_scope:fib.Fib)

class FibList(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _FIBLIST
  
  # @@protoc_insertion_point(class_scope:fib.FibList)

# @@protoc_insertion_point(module_scope)

import struct
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields, field
from typing import Any, Dict, List, Self, Type, TypeVar

@dataclass
class Message:
    message_id: str

def message_field(id: int, **kwargs) -> Any:
    """A decorator for fields that adds an 'id' to their metadata."""
    return field(metadata={
        "id": id,
        "skip": False,
    }, **kwargs)

def message_type(id: str, **kwargs):
    """A decorator for fields that adds an 'id' to their metadata."""
    return field(metadata={
        "skip": True,
    }, default=id, init=False)

M = TypeVar('M', bound='Message')

class MessageRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, message_cls: Type[M]) -> Type[M]:
        """Register a message class."""
        message_id = getattr(message_cls, "message_id", None)
        if not message_id:
            raise ValueError("Message class must have a 'message_id' attribute.")
        self._registry[message_id] = message_cls
        return message_cls

    def __call__(self, message_cls: Type[M]) -> Type[M]:
        """Decorator for registering message classes."""
        return self.register(message_cls)

    def encode(self, message: M) -> bytes:
        """Encode a message into binary format."""
        message_type_id = message.message_id.encode("utf-8")
        payload = struct.pack("!I", len(message_type_id)) + message_type_id
        for field in fields(message):
            if field.metadata.get("skip"):
                continue

            field_id = field.metadata.get("id")
            if field_id is None:
                raise ValueError(f"Field '{field.name}' in message '{message.message_id}' must have an 'id'.")
            value = getattr(message, field.name)
            if isinstance(value, int):
                payload += struct.pack("!BI", field_id, value)  # Field ID + int value
            elif isinstance(value, str):
                encoded_value = value.encode("utf-8")
                payload += struct.pack("!BI", field_id, len(encoded_value)) + encoded_value  # Field ID + str value
            else:
                raise TypeError(f"Unsupported type: {type(value)}")
        return payload

    def decode(self, data: bytes) -> Message:
        """Decode binary data into a message instance."""
        offset = 0
        # Decode the message type ID
        type_id_length, = struct.unpack_from("!I", data, offset)
        offset += 4
        type_id = data[offset:offset + type_id_length].decode("utf-8")
        offset += type_id_length

        if type_id not in self._registry:
            raise ValueError(f"Unknown message ID: {type_id}")

        message_cls = self._registry[type_id]
        field_map = {
            field.metadata["id"]: field
            for field in fields(message_cls)
            if not field.metadata["skip"]
        }
        field_values = {}

        # Decode fields by their IDs
        while offset < len(data):
            field_id, = struct.unpack_from("!B", data, offset)
            offset += 1
            if field_id not in field_map:
                raise ValueError(f"Unknown field ID: {field_id} in message {type_id}")
            field = field_map[field_id]
            if field.type == int:
                value, = struct.unpack_from("!I", data, offset)
                offset += 4
            elif field.type == str:
                length, = struct.unpack_from("!I", data, offset)
                offset += 4
                value = data[offset:offset + length].decode("utf-8")
                offset += length
            else:
                raise TypeError(f"Unsupported type: {field.type}")
            field_values[field.name] = value

        return message_cls(**field_values)

echo_request = MessageRegistry()
echo_response = MessageRegistry()

@echo_request
@dataclass
class CloseRequest(Message):
    message_id: str = message_type('base:close')

@echo_request
@dataclass
class HandshakeRequest(Message):
    message_id: str = message_type('base:handshake')

@echo_response
@dataclass
class HandshakeResponse(Message):
    message_id: str = message_type('base:handshake')

@echo_request
@dataclass
class EchoRequest(Message):
    message_id: str = message_type('app:echo')
    message: str = message_field(1)

@echo_response
@dataclass
class EchoResponse(Message):
    message_id: str = message_type('app:echo')
    message: str = message_field(1)





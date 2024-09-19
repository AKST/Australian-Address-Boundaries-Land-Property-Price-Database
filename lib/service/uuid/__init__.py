import uuid

class UuidService:
    def get_uuid4_hex(self):
        return uuid.uuid4().hex

import uuid
import hashlib


def generate_custom_uuid(id, name, birthdate):
    # Concatenate ID, name, and birthdate
    data = f"{id}{name}{birthdate}"

    # Calculate MD5 hash of the concatenated data
    hash_object = hashlib.md5(data.encode())

    # Generate a UUID from the MD5 hash
    custom_uuid = uuid.UUID(hash_object.hexdigest())
    return custom_uuid

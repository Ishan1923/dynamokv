import hashlib


def h_int(text):
    # hash -> big integer (ring position)
    return int(hashlib.md5(text.encode()).hexdigest(), 16)


def h_hex(text):
    # hash -> hex string (merkle digests)
    return hashlib.md5(text.encode()).hexdigest()

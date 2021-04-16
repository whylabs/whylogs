from whylogs.util.varint import encode, decode_bytes


def test_encode_decode():

    number = 12323244646
    varint_encoded = encode(number)
    decoded_number = decode_bytes(varint_encoded)
    assert number == decoded_number


def test_decode_eof():

    eos = b""
    decoded_eos = decode_bytes(eos)
    assert decoded_eos is None

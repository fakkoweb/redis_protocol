#!/usr/bin/env python
# -*- coding: utf-8 -*-

SIMPLE_STR_TYPE = b"+"
ERROR_TYPE = b"-"
INTEGER_TYPE = b":"
BULK_STR_TYPE = b"$"
ARRAY_TYPE = b"*"
DELIMITER = b"\r\n"


def encode(obj):
    "Pack a series of arguments into a value Redis command"
    if isinstance(obj, tuple) or isinstance(obj, list):
        return encode_array(obj)
    if isinstance(obj, int):
        return encode_integer(obj)
    if isinstance(obj, str):
        obj = obj.encode('utf8')
    if not isinstance(obj, bytes):
        raise TypeError("Cannot encode '{}' type".format(type(obj).__name__))
    return encode_bulk_str(obj)


def encode_integer(i):
    result = []
    result.append(INTEGER_TYPE)
    result.append(str(i).encode('utf8'))
    result.append(DELIMITER)
    return b"".join(result)


def encode_bulk_str(s):
    result = []
    result.append(BULK_STR_TYPE)
    result.append(bytes(str(len(s)).encode('utf8')))
    result.append(DELIMITER)
    result.append(s)
    result.append(DELIMITER)
    return b"".join(result)


def encode_array(array):
    result = []
    result.append(ARRAY_TYPE)
    result.append(bytes(str(len(array)).encode('utf8')))
    result.append(DELIMITER)
    for element in array:
        result.append(encode(element))
    return b"".join(result)


def decode_stream(data):
    result = []
    size = len(data)
    start = 0
    while start < size:
        decoded, index = decode(data[start:], extra=True)
        result.append(decoded)
        start += index
    return result


def decode(data, extra=False):
    if not isinstance(data, bytes):
        msg = "a bytes-like object is required, not '{}'"
        raise TypeError(msg.format(type(data).__name__))
    type_ = data[0: 1]
    if type_ == ARRAY_TYPE:
        result, index = decode_array(data)
    elif type_ == BULK_STR_TYPE:
        result, index = decode_bulk_str(data)
    elif type_ == SIMPLE_STR_TYPE:
        result, index = decode_simple_str(data)
    elif type_ == ERROR_TYPE:
        result, index = decode_simple_str(data)
    elif type_ == INTEGER_TYPE:
        result, index = decode_integer(data)
    else:
        raise TypeError("Unknown type: {}".format(type_))
    if extra:
        result = (result, index)
    return result


def decode_array(data):
    result = []
    start = data.find(DELIMITER)
    size = int(data[1: start])
    start += len(DELIMITER)
    for _ in range(size):
        decoded, index = decode(data[start:], extra=True)
        result.append(decoded)
        start += index + len(DELIMITER)
    return result, start


def decode_bulk_str(data):
    end = data.find(DELIMITER)
    size = int(data[1: end])
    start = end + len(DELIMITER)
    end = start + size
    return data[start: end], end


def decode_simple_str(data):
    end = data.find(DELIMITER)
    result = data[1: end]
    return result, end


def decode_integer(data):
    end = data.find(DELIMITER)
    result = int(data[1: end])
    return result, end


if __name__ == '__main__':
    print(decode(encode("ping")))
    print((encode("set some value")))
    print(encode("foobar"))
    data = b'*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-7630684\r\n$3\r\nAAA\r\n'
    print(decode(data))
    print(decode_stream(data))

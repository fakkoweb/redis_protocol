#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io

SIMPLE_STR_TYPE = b"+"
ERROR_TYPE = b"-"
INTEGER_TYPE = b":"
BULK_STR_TYPE = b"$"
ARRAY_TYPE = b"*"
DELIMITER = b"\r\n"
CHUNK_SIZE = 8192


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


def find_delimiter(data):
    index = data.find(DELIMITER)
    if index == -1:
        raise EOFError("Could not find delimiter")
    return index


def decode_stream(data, show_index=False):
    result = []
    size = len(data)
    index = 0
    while index < size:
        decoded, bytes_used = decode(data[index:], show_index=True)
        result.append(decoded)
        index += bytes_used
    if show_index:
        result = (result, index)
    return result


def decode(data, show_index=False):
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
    if show_index:
        result = (result, index)
    return result


def decode_array(data):
    result = []
    start = find_delimiter(data)
    count = int(data[1: start])
    if count < 0:
        return None, -1
    start += len(DELIMITER)
    if count > 0:
        for _ in range(count):
            if start >= len(data):
                raise EOFError("Not enough data in buffer to decode array")
            decoded, index = decode(data[start:], show_index=True)
            result.append(decoded)
            start += index
    return result, start


def decode_bulk_str(data):
    end = find_delimiter(data)
    size = int(data[1: end])
    if size < 0:
        return None, -1
    start = end + len(DELIMITER)
    end = start + size
    string = str((data[start: end]).decode('utf8'))
    if len(string) != size:
        raise EOFError("Not enough data in buffer to decode string")
    if data[end: end + len(DELIMITER)] != DELIMITER:
        raise TypeError("Buffer is not properly terminated")
    return string, end + len(DELIMITER)


def decode_simple_str(data):
    end = find_delimiter(data)
    result = str((data[1: end]).decode('utf8'))
    return result, end + len(DELIMITER)


def decode_integer(data):
    end = find_delimiter(data)
    result = int(data[1: end])
    return result, end + len(DELIMITER)


if __name__ == '__main__':
    print(decode(encode("ping")))
    print((encode("set some value")))
    print(encode("foobar"))
    data = b'*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-7630684\r\n$3\r\nAAA\r\n'
    print(decode(data))
    print(decode_stream(data))

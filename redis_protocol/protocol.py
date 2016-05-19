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
        raise TypeError("Cannot encode {} types".format(type(obj)))
    return encode_bulk_str(obj)


def encode_integer(i):
    result = []
    result.append(INTEGER_TYPE)
    result.append(str(i).encode('utf-8'))
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


def decode(data):
    processed, index = 0, data.find(DELIMITER)
    if index == -1:
        index = len(data)
    term = data[processed]
    if term == ARRAY_TYPE:
        return decode_array(data)
    elif term == BULK_STR_TYPE:
        return decode_bulk_str(data)
    elif term == SIMPLE_STR_TYPE:
        return decode_simple_str(data)
    elif term == ERROR_TYPE:
        return decode_error(data)
    elif term == INTEGER_TYPE:
        return decode_integer(data)


def decode_stream(data):
    cursor = 0
    data_len = len(data)
    result = []
    while cursor < data_len:
        pdata = data[cursor:]
        index = pdata.find(DELIMITER)
        count = int(pdata[1:index])

        cmd = ''
        start = index + len(DELIMITER)
        for i in range(count):
            chunk, length = parse_chunked(pdata, start)
            start = length + len(DELIMITER)
            cmd += " " + chunk
        cursor += start
        result.append(cmd.strip())
    return result


def parse_multi_chunked(data):
    index = data.find(DELIMITER)
    count = int(data[1:index])
    result = []
    start = index + len(DELIMITER)
    for i in range(count):
        chunk, length = parse_chunked(data, start)
        start = length + len(DELIMITER)
        result.append(chunk)
    return result


def parse_chunked(data, start=0):
    index = data.find(DELIMITER, start)
    if index == -1:
        index = start
    length = int(data[start + 1:index])
    if length == -1:
        if index + len(DELIMITER) == len(data):
            return None
        else:
            return None, index
    else:
        result = data[index + len(DELIMITER):index + len(DELIMITER) + length]
        return result if start == 0 else [result, index + len(DELIMITER) + length]


def parse_status(data):
    return [True, data[1:]]


def parse_error(data):
    return [False, data[1:]]


def parse_integer(data):
    return [int(data[1:])]


if __name__ == '__main__':
    print(decode(encode("ping")))
    print((encode("set some value")))
    print(encode("foobar"))
    data = '*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-8232902\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$15\r\nmemtier-7630684\r\n$3\r\nAAA\r\n'
    print(parse_stream(data))

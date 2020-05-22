#!/usr/bin/env python3
# vim: et ts=8 sts=4 sw=4

import datetime
import sys, json, re, os, os.path, shutil
import logging, configparser
import functools, itertools, traceback, hashlib

def concat_dataframes(dataframes):
    return functools.reduce(lambda a, b: a.append(b), dataframes)

def md5(obj, truncate_to_length: int = 0):
    s = str(obj)
    b = s.encode()
    md5er = hashlib.md5()
    md5er.update(b)
    digest = md5er.digest()
    if truncate_to_length > 0:
        truncated = digest[:truncate_to_length]
    else:
        truncated = digest
    return int.from_bytes(truncated, byteorder="big")

def now_timestamp():
    ts = datetime.datetime.now().isoformat()
    ts = re.sub(r"[^\d]", "", ts)
    return ts[:14]


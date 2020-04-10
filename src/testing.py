#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import datetime
import sys, json, re, os, os.path
import logging

def lists_equal(a, b):
    if len(a) != len(b):
        return False
    for ai in range(len(a)):
        for bi in range(len(b)):
            print(ai, bi)
            ax = a[ai]
            bx = b[bi]
            if a[ai] == b[bi]:
                del b[bi]
                break
        else:
            raise ValueError("Unmatched index: %d" % ai)
    return True

def test_lists_equal():
    assert lists_equal([1,2,3], [3,2,1])
    assert not lists_equal([1,2,3],[1,2,2])



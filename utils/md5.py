# coding:utf-8
__author__ = "zkp"
# create by zkp on 2022/2/8
import hashlib


def md5(_s:str):
    a = hashlib.md5(_s.encode()).hexdigest()
    return a

if __name__ == '__main__':
    aa = md5("ss")
    print([aa])
# coding:utf-8
__author__ = "zkp"
# create by zkp on 2022/6/16
from urllib.parse import *

class Request(object):
    def __init__(self, raw:bytes):
        self.raw = raw
        self.method,self.path,self.headers,self.content,self.form,self.query,self.full_path = self._parse(raw)

    def _parse(self, raw:bytes):
        header,content = raw.split(b"\r\n\r\n",1)
        _, header = header.split(b"\r\n", 1)
        method,full_path = _.split()[:2]
        headers_dict = dict([[j.decode('utf-8') for j in i.split(b": ",1)] for i in  header.split(b"\r\n")])
        method = method.decode("utf-8")
        full_path = full_path.decode("utf-8")
        if '?' in full_path:
            path, query = full_path.split("?")[0],dict(parse_qsl(full_path.split("?")[1]))
        else:
            path,query = full_path,{}
        #print(method, path, headers_dict, content)
        #query = dict(parse_qsl(path.split("?")[1]))
        #query =
        if method == 'POST':
            form = {i.decode('utf-8'):j.decode("utf-8") for i,j in parse_qsl(content)}
        else:
            form = {}
        return method, path, headers_dict, content, form, query,full_path


if __name__ == '__main__':
    a = Request(b'GET /urlmode/41000000112008000141/41000000112008000141/TCP.flv HTTP/1.1\r\nHost: 192.168.1.122:8008\r\nConnection: keep-alive\r\nCache-Control: max-age=0\r\nUpgrade-Insecure-Requests: 1\r\nUser-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7\r\nAccept-Encoding: gzip, deflate\r\nAccept-Language: zh,en;q=0.9,zh-CN;q=0.8\r\nCookie: token=AOqwRC24Hq+Pd/bQtBRF6iFTYU8+jvlCC29VhSNWD+jTKeK+uxCtyHu/ZxP9lP57ZplLcY3SgAJRvdjSgwYkHfj/Qsi0qhydVyzu/xPA1f8S8WKGU1joTX/X0uoCDh+OerRfGPtjyBkUuEHH8rvjNsuS8r4GZB+wXQo6Tpq3KH0AhYgk9x/HsnhX0bTYHcXWSyzQ91SUapjozydDB3xv6sZmeVZczG9JgX0npO2lVyjOQrRKuEXfI4mNbUHsE2Dl/zVnhpzsuZZOTyNGmB4c4knRlbPCBvmxYNlTIQ49r0kcYvT1SfHC6woXUEPTSZ7pco8VF7mI2BBmEf5cEZboiAISYNJYdiMaiK/lDvIUEgRfGoxdeg5ZarSmV63gkdXrG8eNFh3acrgMEOz6n9zV4lmkD3BqVp7e528sab9zOav1O2uO4cNoHBAIdX0E8iNcvrWEFk0RBLdP5FZ+sts6jeTtYav8P6djpWbVZHH0s44/KK20k9tFMTCputzfB0DO09c9tIO9I+AmUB97QUzcTFG0aPsVMjNlg+KfTGUyUVg=; JSESSIONID=88908FC9279605D9C8617CF7BD7D0A21\r\n\r\n')
    print(a.path, a.query,a.form,a.full_path)
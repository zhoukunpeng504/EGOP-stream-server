# coding:utf-8
__author__ = "zkp"
# create by zkp on 2025/1/17
# rtmp(可通过obs推流) ----> httpflv
from gevent.monkey import patch_all;patch_all(thread=False,subprocess=False)
import gevent
from gevent.pool import Pool
import traceback
import psutil
from urllib import parse
import socket
import random
import os
import setproctitle
import hashlib, base64, struct, time
import requests
import datetime
import redis
from utils import process_task, _av_rtmp, request_utils
import re
# import urllib.parse
# encrypt 111



os.system("mkdir -p /data/rtp_streamer")


my_recv_stream_ip = ''


def print_to_logger(*args):
    now = datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')
    try:
        msg = " ".join([str(i) for i in args ])
        print(f"[{now}: INFO]:{msg}")
        #sys.stdout.flush()
    except Exception as e:
        print("logger err!!", str(e))
        pass
    sys.stdout.flush()


def handle(sock, address):
    print_to_logger("connected", os.getpid(), sock, address, time.time())
    request_info = b''
    try:
        with gevent.Timeout(2):
            while 1:
                request_info += sock.recv(1024 * 1024)
                if b'\r\n\r\n' in request_info:
                    _ = re.search(br"[cC]ontent-[lL]ength: (\d+)", request_info)
                    # print("_", _)
                    if _:
                        length = int(_.groups()[0])
                        content = request_info.split(b'\r\n\r\n')[1]
                        if len(content) >= length:
                            break
                    else:
                        break
                gevent.sleep(0.01)
    except (Exception, BaseException) as e:
        print_to_logger("connection read header failed!!", sock, str(e), request_info)
    # print(sock, time.time(), "request_info", request_info)

    try:
        request = request_utils.Request(request_info)
    except Exception as e:
        print_to_logger("error! parse http request failed !", str(e))
        return

    if request.path == '/play.flv':
        # 支持 flv

        cam_url = request.query.get("cam_url", '')
        print_to_logger("request.query", request.query)

        # not is_websocket:
        sock.send(b"HTTP/1.1 200 OK\r\n" +
                  b"Access-Control-Allow-Methods: GET, OPTIONS\r\n"+
                  b"Access-Control-Allow-Origin: *\r\n"+
                  b"Access-Control-Allow-Credentials: true\r\n" +
                  b"Content-Type: video/x-flv\r\n"+
                  b"\r\n")

        print_to_logger("send http header", time.time())

        assert cam_url   # 必须传其中一个参数
        redis_conn = redis.Redis.from_url(redis_url)
        queue_key = hashlib.md5(f"httpflv_queue_{cam_url}_{random.random()}".encode('utf-8')
                                ).hexdigest()[:10]
        target_url = cam_url
        print_to_logger("target_url", target_url)
        md5_target_url = hashlib.md5(target_url.encode('utf-8')).hexdigest()[:10]
        sub_scribe_key = md5_target_url
        print_to_logger(target_url, "sub_scribe_key", sub_scribe_key,
                        'queue_key', queue_key)
        # 启动mux进程
        task_id = process_task.run_task(_av_rtmp.av_mux_function,
                                        (sub_scribe_key,
                                         redis_url,
                                         queue_key))


        last_data_time = time.time()
        print_to_logger("last_data_time", last_data_time)
        while 1:
            # 如果socket被关闭 或者 超过1.5分钟未获取到数据
            if time.time() - last_data_time > 10:
                print_to_logger("last data time > 10 s  end .....", last_data_time)
                break
            try:
                info = redis_conn.lpop(queue_key)
                assert len(info) > 0
                # gevent.sleep(0.001)
            except Exception as e:
                gevent.sleep(0.03)
            else:
                last_data_time = time.time()
                try:
                    sock.sendall(info)
                except Exception as e:
                    print_to_logger("sock.sendall error", str(e))
                    break
        print_to_logger("end.....")
        try:
            with open(f"/tmp/process_task/pids/{task_id}", "r") as f:
                worker_pid = int(f.read())
        except:
            print_to_logger("cant find pid!")
        else:
            try:
                p_obj = psutil.Process(pid=worker_pid)
                print_to_logger("rtp_mux process pid found", p_obj)
                p_obj.send_signal(9)
                p_obj.kill()
            except Exception as e:
                print_to_logger(str(e))
                pass
            try:
                p_obj.wait()
            except:
                pass
        redis_conn.delete(queue_key)
        redis_conn.close()



# 僵尸进程清理函数
def process_clean_fun():
    print_to_logger("process clean fun start ....", time.time())
    while 1:
        for child in psutil.Process(os.getpid()).children(recursive=True):
            try:
                if child.status() in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD):
                    child.send_signal(9)
                    child.kill()
                    child.wait()
            except Exception as e:
                print_to_logger("process clean fun error", str(e))
        gevent.sleep(1)


if __name__ == '__main__':
    import sys
    # 配置文件解析

    httpflv_port = os.environ.get('HTTPFLV_PORT', None) or 8009
    redis_url = os.environ.get("REDIS_URL", 'redis://127.0.0.1:6379/9')
    # 进程title设置
    setproctitle.setproctitle("rtmp_flv_server")
    pool = Pool(3000)
    from gevent.server import StreamServer
    server = StreamServer(('', int(httpflv_port)), handle, spawn=pool)
    server.start()
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024*32)
    try:
        redis_con = redis.Redis.from_url(redis_url) # .ping()
        assert redis_con.ping() == True
        # 删除原有数据
        redis_con.flushdb()
    except:
        raise Exception("redis 无法连接！" + redis_url)
    print(f'httpflv_server start {httpflv_port}..', time.time())
    print("redis_url", redis_url)

    for i in range(10):
        print("start rtmp server", f'/live/chan{i}')
        process_task.run_task(_av_rtmp.av_recv_function, args=[f'{ 8010+i }',
                                                                f'/live/chan{i}',
                                                                hashlib.md5(f'/live/chan{i}'.encode('utf-8')).hexdigest()[:10],
                                                                redis_url,
                                                                f"rtmp_chan{i}"
                                                                ])
    greenlet_1 = gevent.spawn(process_clean_fun)
    server.start()
    # fork n次， 产生n个子进程。 一共n+1个进程。
    for i in range(3):
        res = os.fork()
        if res == 0:
            break
    while True:
        gevent.sleep(1)
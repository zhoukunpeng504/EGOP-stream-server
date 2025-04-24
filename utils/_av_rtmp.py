# coding:utf-8
__author__ = "zkp"
# create by zkp on 2022/8/4
# import os,sys
# _ = os.path.abspath(os.path.join(os.path.dirname(__file__)))
# sys.path.append(_)


def av_recv_function(rtmp_port, rtmp_path, sub_scribe_key, redis_url, process_name):
    import time, redis, os, datetime, psutil, cuda_available,fractions
    def print_to_logger(*args):
        "日志函数"
        file_name = os.path.join("/data/http_rtmp_streamer",
                                 f"rtmp_recv-{datetime.datetime.now().strftime('%Y%m%d')}.log")
        now = datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')
        try:
            msg = " ".join([str(i) for i in args])
            with open(file_name, "a+") as f:
                f.write(f"[{now}: INFO]:{msg}\n")
        except:
            pass

    redis_conn = redis.Redis.from_url(redis_url)

    # 开始正常业务处理流程
    import av
    av.logging.set_level(av.logging.CRITICAL)
    print("start av_recv_function", time.time())
    import time
    import sys
    import traceback, random, string
    import psutil
    import pickle, json
    import os
    from av import bitstream

    trans_gop_size = os.environ.get('GOPSIZE', None) or os.environ.get('GOP_SIZE', None) or '50'
    trans_gop_size = int(trans_gop_size)
    # import ctypes
    current_dir = os.path.abspath(os.path.dirname(__file__))
    sys.path.append(current_dir)
    try:
        import md5
    except:
        from . import md5
    # 删除老的标识文件
    import setproctitle
    setproctitle.setproctitle(process_name)
    os.system("mkdir -p /data/http_rtmp_streamer")
    output_kw_params = {"video": {},
                        "audio": {},
                        'nv_hw': None}  # 输出flv核心参数

    with open(os.path.abspath(os.path.join(current_dir, "..", "config.json")), "r") as f:
        kwargs = json.loads(f.read())
    # kwargs = {"format": 'flv',
    #           "container_options":
    #               container_options
    #           }
    print_to_logger("kwargs", kwargs)
    all_codecs = av.codecs_available
    if (cuda_available.getCudaDeviceCount() and 'hevc_nvenc' in all_codecs
            and 'h264_cuvid' in all_codecs):
        nv_hw = True
    else:
        nv_hw = False
    output_kw_params['nv_hw'] = nv_hw
    print_to_logger("nvidia hw codec status", nv_hw)
    rtmp_url = f'rtmp://0.0.0.0:{rtmp_port}{rtmp_path}'

    def get_fraction_obj(time_base:str):
        _a,_b = time_base.split('/')
        _a,_b = int(_a),int(_b)
        return fractions.Fraction(_a,_b)

    while True:
        output_kw_params['video'] = {}
        output_kw_params['audio'] = {}
        try:
            print_to_logger("av open", rtmp_url)
            input_av = av.open(rtmp_url, 'r',
                            #metadata_errors='ignore',
                            # format=format,
                            timeout=None,
                            **kwargs
                            )
            print_to_logger("av open ...")
            print_to_logger("input_av flags", input_av.flags)
            video = input_av.streams.video[0]
            mp4toannexb_filter = bitstream.BitStreamFilterContext("h264_mp4toannexb",
                                                        in_stream=video)
            code_name = video.codec_context.name
            print_to_logger("video code name is", code_name, video.codec_context.pix_fmt,
                            video.codec_context.profile)
            print_to_logger("#######")
            # _ =
            # need_encode = False
            # h264_codec = None
            def get_profile(codec_context):
                if not codec_context.profile:
                    if '444p' not in codec_context.pix_fmt:
                        return 'high'
                    else:
                        return 'high444p'
                return 'high'

            if code_name in ('h265', 'hevc'):
                # h264_codec_r = None
                output_kw_params['video']['codec'] = 'h264'
                output_kw_params['video']['width'] = video.width # 1920 if video.width >= 1920 else video.width
                output_kw_params['video']['height'] = video.height  #1080 if video.height >= 1080 else video.height
                output_kw_params['video']['pix_fmt'] = 'yuv420p' #video.codec_context.pix_fmt
                output_kw_params['video']['time_base'] = f"{video.time_base.numerator}/{video.time_base.denominator}"
                output_kw_params['video']['profile'] = 'high'  #get_profile(video.codec_context)
                if video.base_rate.numerator <= 100:
                    output_kw_params['video']['rate'] = f"{video.base_rate.numerator}/{video.base_rate.denominator}"
                else:
                    output_kw_params['video']['rate'] = f"25/1"

                if not nv_hw:
                    h264_codec_w = av.CodecContext.create('h264', "w")
                    h264_codec_w.pix_fmt = output_kw_params['video']['pix_fmt']
                    h264_codec_w.width = output_kw_params['video']['width']
                    h264_codec_w.height = output_kw_params['video']['height']
                    h264_codec_w.time_base = get_fraction_obj(output_kw_params['video']['time_base'])
                    h264_codec_w.gop_size = trans_gop_size  # 设置gop 为25
                    h264_codec_w.options = {"level": '42',
                                          'profile': 'high',
                                          'tune': 'zerolatency',
                                          'preset': 'ultrafast',
                                          'crf': '17',   # 预置视频画质标准
                                          'threads': '10',
                                          #'vf': '''"format=nv12"'''   # 强制转化为yuv420p
                                            'rc': 'vbr',
                                            'rgb_mode': '1'
                                          }
                else:
                    h264_codec_w = av.CodecContext.create('h264_nvenc', "w")
                    h264_codec_w.pix_fmt = output_kw_params['video']['pix_fmt']
                    h264_codec_w.width = output_kw_params['video']['width']
                    h264_codec_w.height = output_kw_params['video']['height']
                    h264_codec_w.time_base = get_fraction_obj(output_kw_params['video']['time_base'])
                    h264_codec_w.gop_size = trans_gop_size  # 设置gop
                    h264_codec_w.flags = 'LOW_DELAY'
                    h264_codec_w.options = {
                        'preset': '15',  # 'p4',
                        'tune': '3',  # "ull",
                        'delay': '0',
                        'zerolatency': '1',
                        'rc': '1',
                        'profile': 'high',
                        # 'rgb_mode': '1'
                    }

                h264_codec_w.color_primaries = video.codec_context.color_primaries
                h264_codec_w.color_range = video.codec_context.color_range
                h264_codec_w.color_trc = video.codec_context.color_trc
                h264_codec_w.colorspace = video.codec_context.colorspace
                output_kw_params['video']['color_primaries'] = h264_codec_w.color_primaries
                output_kw_params['video']['color_range'] = h264_codec_w.color_range
                output_kw_params['video']['color_trc'] = h264_codec_w.color_trc
                output_kw_params['video']['colorspace'] = h264_codec_w.colorspace

            else:
                output_kw_params['video']['codec'] = 'h264'
                output_kw_params['video']['width'] = video.width
                output_kw_params['video']['height'] = video.height
                output_kw_params['video']['pix_fmt'] = video.codec_context.pix_fmt
                output_kw_params['video']['time_base'] = f"{video.time_base.numerator}/{video.time_base.denominator}"
                if video.base_rate.numerator <= 100:
                    output_kw_params['video']['rate'] = f"{video.base_rate.numerator}/{video.base_rate.denominator}"
                else:
                    output_kw_params['video']['rate'] = f"25/1"
                output_kw_params['video']['color_primaries'] = video.codec_context.color_primaries
                output_kw_params['video']['color_range'] = video.codec_context.color_range
                output_kw_params['video']['color_trc'] = video.codec_context.color_trc
                output_kw_params['video']['colorspace'] = video.codec_context.colorspace

                output_kw_params['video']['profile'] = get_profile(video.codec_context)
                h264_codec_w = None
            try:
                audio = input_av.streams.audio[0]
                audio_code_name = audio.codec_context.name
                output_kw_params['audio']['codec'] = audio_code_name
                output_kw_params['audio']['rate'] = audio.codec_context.rate
                output_kw_params['audio']['layout'] = audio.codec_context.layout.name
                output_kw_params['audio']['time_base'] = f"{audio.time_base.numerator}/{audio.time_base.denominator}"
                output_kw_params['audio']['bit_rate'] = audio.codec_context.bit_rate
                output_kw_params['audio']['format'] = audio.codec_context.format.name
            except Exception as e:
                print_to_logger("input_av try get  audio error! 252", str(e))
                print_to_logger(traceback.format_exc())
                audio = None
                audio_code_name = ''

            print_to_logger("output_kw_params", output_kw_params)

            print_to_logger("video # audio", video, audio)

            redis_conn.set(f"{sub_scribe_key}-params", json.dumps(output_kw_params))
            redis_conn.delete(f'{sub_scribe_key}-cache')
            v_counter = 0
            a_counter = 0
            late_iframe_counter = 0
            find_iframe_v = False
            for _p in input_av.demux():
                packet_time = time.time()
                # 应对某些视频流第一个packet不是关键帧
                if find_iframe_v == False:
                    if _p.is_keyframe:
                        find_iframe_v = True
                    else:
                        continue
                if _p.pts == None and _p.dts == None:
                    _p.pts = 0
                    _p.dts = 0
                else:
                    _p.pts = _p.dts = (_p.dts or _p.pts) + 3600
                # ########
                # _p.dts = _p.pts
                if _p.stream.type == 'video':
                    # 视频帧
                    target_packet = None
                    if code_name not in ('h265', 'hevc'):
                        # 无需转码
                        frames = _p.decode()
                        if frames:
                            target_packet = _p
                            target_frame = frames[0]

                        # v_counter += 1
                    else:
                        # 需要转码
                        target_packet = None
                        frames = _p.decode()
                        if frames:
                            try:
                                # _out_stream.width = 960
                                # _out_stream.height = 540
                                # print(frame)
                                _packets = h264_codec_w.encode(frames[0])
                                for _p in _packets[:1]:
                                    target_packet = _p
                                    target_frame = frames[0]
                            except (Exception, BaseException) as e:
                                print("h264 decode  except:", str(e))
                    if target_packet and target_frame:
                        v_counter += 1
                        if target_packet.is_keyframe:
                            late_iframe_counter = v_counter
                        target_packet = mp4toannexb_filter.filter(target_packet)[0]

                        # print_to_logger("target_packet_v", target_packet,
                        #                 target_packet.is_keyframe, v_counter,
                        #                 late_iframe_counter)


                        # 添加 mp4toannexb_filter，应对RTMP推来的流没有前导码问题
                        packet_bytes = bytes(target_packet)
                        # with open("/tmp/h264-packet.h264", "wb+") as packet_file:
                        #     packet_file.write(packet_bytes)

                        send_data = pickle.dumps(
                                {'packet_bytes': packet_bytes,
                                 'packet_counter': v_counter,
                                 "time": packet_time,
                                 "dts": target_packet.dts,
                                 'pts': target_packet.pts,
                                 "is_key": target_packet.is_keyframe,
                                 "late_iframe_counter": late_iframe_counter,
                                 "packet_type": "video"
                                 }
                            )
                        redis_conn.hset(f'{sub_scribe_key}-cache',
                                        str(v_counter % 500),
                                        pickle.dumps({'frame_ndarray': target_frame.to_ndarray(),
                                                      'frame_format': target_frame.format.name,
                                                      'frame_pts': target_packet.pts})
                                        )

                        try:
                            redis_conn.publish(sub_scribe_key, send_data)
                        except (Exception, BaseException) as e:
                            print_to_logger(f"pub to redis {sub_scribe_key} except:", str(e))
                            print_to_logger(traceback.format_exc())


                if _p.stream.type == 'audio' and audio_code_name != '':
                    # 音频帧
                    a_counter += 1
                    target_packet = _p
                    # print_to_logger("target_packet_a", target_packet,
                    #                 )
                    try:
                        redis_conn.publish(sub_scribe_key, pickle.dumps(
                            {'packet_bytes':  bytes(target_packet),
                             'packet_counter': a_counter,
                             "time": packet_time,
                             "dts": target_packet.dts,
                             'pts': target_packet.pts,
                             "is_key": target_packet.is_keyframe,
                             'packet_type': "audio"
                             }
                        ))
                    except (Exception, BaseException) as e:
                        print_to_logger(f"pub to redis {sub_scribe_key} except:", str(e))
                        print_to_logger(traceback.format_exc())


        except (Exception, BaseException, av.error.FFmpegError) as e:
            print_to_logger("#######", str(e))
            print_to_logger(traceback.format_exc())
        finally:
            try:
                input_av.close()
            except:
                pass
            try:
                h264_codec_w.flush_buffers()
            except:
                pass
            try:
                h264_codec_w.close()
            except:
                pass
        try:
            redis_conn.delete(f"{sub_scribe_key}-params")
            redis_conn.delete(f'{sub_scribe_key}-cache')
            redis_conn.publish(sub_scribe_key, pickle.dumps(
                {
                 'packet_type': "close"
                 }
            ))
            print_to_logger("send redis close msg!!")
        except:
            pass



def av_mux_function(sub_scribe_key, redis_url, queue_key):
    # 把packet mux 到flv中
    import time, av
    from av import codec
    av.logging.set_level(av.logging.CRITICAL)
    print("start av_mux_function ~~ ", time.time())
    import time
    import sys
    import redis, datetime, os, traceback, random, string
    #import psutil
    import pickle, json
    from av import packet
    from av import codec
    import fractions
    # import ctypes
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    try:
        import md5
    except:
        from . import md5
    # 删除老的标识文件
    import setproctitle
    import av.video as av_video

    if sub_scribe_key != '':
        setproctitle.setproctitle(f"rtp_mux_{sub_scribe_key}")
    else:
        setproctitle.setproctitle("rtp_mux")
    # main_p = psutil.Process(main_process_pid)
    os.system("mkdir -p /data/http_ps_streamer")
    my_pid = os.getpid()

    def print_to_logger(*args):
        "日志函数"
        file_name = os.path.join("/data/http_rtmp_streamer",
                                 f"rtp_mux-{datetime.datetime.now().strftime('%Y%m%d')}.log")
        now = datetime.datetime.now().isoformat(sep=' ', timespec='milliseconds')
        try:
            msg = " ".join([str(i) for i in args])
            with open(file_name, "a+") as f:
                f.write(f"[{now}: INFO-{my_pid}]:{msg}\n")
        except:
            pass

    redis_conn = redis.Redis.from_url(redis_url)
    ps = redis_conn.pubsub()
    ps.subscribe(sub_scribe_key)

    class FileObj(object):
        def __init__(self, name):
            self.name = name
            self.key = queue_key
            self.buff = b''
            self.last_time = time.time()

        def write(self, info: bytes):
            #print_to_logger("write ....")
            _t = time.time()
            self.buff += info
            #if _t - self.last_time > 0.03:  # 超过
            try:
                redis_conn.rpush(self.key, self.buff)
                # print("write to redis", len(self.buff))
            except Exception as e:
                print_to_logger("put failed")
                print_to_logger(traceback.format_exc())
            self.buff = b''
            self.last_time = _t

        def qsize(self):
            try:
                # return self.queue.qsize()
                return redis_conn.llen(self.key)
            except:
                return 0

    def get_fraction_obj(time_base:str):
        _a,_b = time_base.split('/')
        _a,_b = int(_a),int(_b)
        return fractions.Fraction(_a, _b)

    av_output = None
    out_stream_v = None
    out_stream_a = None
    # 因为FLV 和 原始视频的 time_base不同。 要通过如下两个变量进行修正
    v_pts_dts_div = 0  # 用于修正视频的PTS DTS
    a_pts_dts_div = 0  # 用于修正音频的PTS DTS
    h264_codec_r = None
    h264_codec_w = None
    nv_hw = None
    v_packet_dur = None
    a_packet_dur = None
    def mux_v(av_out, v_packet):
        nonlocal v_packet_dur
        if v_packet_dur is None:
            v_packet_dur = v_packet.pts
        v_packet.pts = v_packet.pts - v_packet_dur
        v_packet.dts = v_packet.pts
        print_to_logger("mux_v", v_packet)
        av_out.mux(v_packet)

    def mux_a(av_out, a_packet):
        nonlocal a_packet_dur
        if a_packet_dur is None:
            a_packet_dur = a_packet.pts
        a_packet.pts = a_packet.pts - a_packet_dur
        a_packet.dts = a_packet.pts
        print_to_logger("mux_a", a_packet)
        av_out.mux(a_packet)

    v_handle_flag = 0   # 0 代表 无需处理  1 处理中  2 处理完成
    for item in ps.listen():
        if item['type'] == 'message':
            if av_output == None:
                params = json.loads(redis_conn.get(f"{sub_scribe_key}-params"))
                video_params = params['video']
                audio_params = params['audio']
                nv_hw = params['nv_hw']
                print_to_logger("params is ", params)
                # _ =
                av_output = av.open(FileObj("11.flv"), 'w')

                out_stream_v = av_output.add_stream(codec_name=video_params['codec'].split("_")[0], # 把h264_nvenc转化为h264
                                                    rate=int(video_params['rate'].split("/")[0]),
                                                    #options={'time_base': audio_params['time_base']}
                                                    )
                out_stream_v.width = video_params['width']
                out_stream_v.height = video_params['height']
                # out_stream_v.base_rate = video_params['rate']
                # out_stream_v.average_rate = video_params['rate']
                out_stream_v.time_base = fractions.Fraction(1, 1000) #'1/1000'  # video_params['time_base']
                v_pts_dts_div = (int(video_params['time_base'].split("/")[1])/1000)

                if audio_params:
                    # 如果存在音频。 注意：某些视频源无音频信息。
                    out_stream_a = av_output.add_stream(codec_name=audio_params['codec'],
                                                        rate=audio_params['rate'],
                                                        layout=audio_params['layout'],
                                                        #options={'time_base': audio_params['time_base']}
                                                        )
                    out_stream_a.time_base = fractions.Fraction(1, 1000) #'1/1000'  # audio_params['time_base']
                    a_pts_dts_div = (int(audio_params['time_base'].split("/")[1])/1000)


                # h264_codec_r.gop_size = video_params['gop_size']
                # profile = video_params['profile']
                pix_fmt = video_params['pix_fmt']
                if pix_fmt == 'yuv444p':
                    w_profile = 'high444p'
                else:
                    w_profile = 'high'
                if nv_hw and pix_fmt in ('yuv420p', 'yuv444p'):
                    h264_codec_w = av.CodecContext.create('h264_nvenc', "w")
                    h264_codec_w.flags = 'LOW_DELAY'
                    h264_codec_w.pix_fmt = pix_fmt
                    h264_codec_w.width = video_params['width']
                    h264_codec_w.height = video_params['height']
                    h264_codec_w.time_base = get_fraction_obj(video_params['time_base']) #video_params['time_base']
                    h264_codec_w.options = {
                            #'preset': 'p7',
                            'tune': '2',
                            'delay': '0',
                            #'zerolatency': 'true',
                            'crf': '3',
                            'profile':  w_profile, #'high',  # '3' if  pix_fmt == 'yuv420p' # 'str(profile).lower(),
                            }
                else:
                    h264_codec_w = av.CodecContext.create('h264', "w")
                    h264_codec_w.pix_fmt = pix_fmt
                    h264_codec_w.width = video_params['width']
                    h264_codec_w.height = video_params['height']
                    h264_codec_w.time_base = get_fraction_obj(video_params['time_base'])  # video_params['time_base']
                    h264_codec_w.options = {"level": '42',
                                            # 'Profile': 'High',
                                            'tune': 'zerolatency',
                                            'preset': 'ultrafast',
                                            'crf': '10',
                                            'threads': '10',
                                            #'color_primaries': str(color_primaries),
                                            #'color_range': str(color_range),
                                            #'color_trc': str(color_trc),
                                            #'colorspace': str(colorspace),
                                            'profile': w_profile, #'3', # str(profile).lower(),
                                            'rc': "vbr",
                                            #'rgb_mode': "1"
                                            }
                # h264_codec_w.gop_size = 25  # 设置gop 为25
                # h264_codec_w.gop_size = video_params['gop_size']
                color_primaries = video_params['color_primaries']
                color_range = video_params['color_range']
                color_trc = video_params['color_trc']
                colorspace = video_params['colorspace']
                h264_codec_w.color_primaries = int(color_primaries)
                # h264_codec_w.gop_size = 25  # 设置gop 为25
                h264_codec_w.bit_rate = 4096000
                h264_codec_w.color_range = int(color_range)
                h264_codec_w.color_trc = int(color_trc)
                h264_codec_w.colorspace = int(colorspace)
                h264_codec_w.rate = get_fraction_obj(video_params['rate'])
                h264_codec_w.framerate = get_fraction_obj(video_params['rate'])
                print_to_logger("### h264_codec_w is ", h264_codec_w)

            item_data = pickle.loads(item['data'])
            if item_data['packet_type'] == 'video':
                v_packet = packet.Packet(item_data['packet_bytes'])
                v_packet.pts = item_data['pts']
                v_packet.dts = item_data['dts']
                v_packet.is_keyframe = item_data['is_key']
                packet_counter = item_data['packet_counter']
                late_iframe_counter = item_data['late_iframe_counter']
                # buffer_packets = []
                if late_iframe_counter != packet_counter and v_handle_flag in (0, 1):
                    # 需要特殊处理。
                    # 从cache中取出frame.
                    # 此部分是关键逻辑
                    cache_data = pickle.loads(redis_conn.hget(f'{sub_scribe_key}-cache',
                                    str(packet_counter % 500)))
                    v_frame = av_video.frame.VideoFrame.from_ndarray(cache_data['frame_ndarray'],
                                                                     format=cache_data['frame_format'])
                    v_frame.pts = cache_data['frame_pts']
                    v_frame.dts = v_frame.pts
                    v_handle_flag = 1
                    decode_res_1 = h264_codec_w.encode(v_frame)
                    print_to_logger("decode_res_1 ", decode_res_1)
                    if decode_res_1:
                        decode_res_1[0].stream = out_stream_v
                        decode_res_1[0].pts = decode_res_1[0].pts / v_pts_dts_div
                        decode_res_1[0].dts = decode_res_1[0].dts
                        print_to_logger("decode_res_1[0]", decode_res_1[0])
                        # av_output.mux(decode_res_1[0])
                        mux_v(av_output, decode_res_1[0])
                    print_to_logger("handle frame ...###")
                else:
                    try:
                        h264_codec_w.flush_buffers()
                    except:
                        pass
                    try:
                        h264_codec_w.close()
                    except:
                        pass
                    v_handle_flag = 2
                    v_packet.stream = out_stream_v
                    v_packet.pts = v_packet.pts / v_pts_dts_div
                    v_packet.dts = v_packet.dts / v_pts_dts_div
                    mux_v(av_output, v_packet)

                print_to_logger('v_packet', v_packet, packet_counter, late_iframe_counter, v_packet.is_keyframe)

            elif item_data['packet_type'] == 'audio':

                a_packet = packet.Packet(item_data['packet_bytes'])
                a_packet.pts = item_data['pts'] / a_pts_dts_div
                a_packet.dts = item_data['dts'] / a_pts_dts_div
                a_packet.is_keyframe = item_data['is_key']
                a_packet.stream = out_stream_a
                mux_a(av_output, a_packet)
                print_to_logger('a_packet', a_packet)

            elif item_data['packet_type'] == 'close':
                # 源关闭, 停止当前
                print_to_logger('packet_type close!!!!')
                print_to_logger('return 0')
                return 0

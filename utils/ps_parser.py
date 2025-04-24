# coding:utf-8
__author__ = "zkp"
# create by zkp on 2021/12/30
# 测试ps frame 转 h264
import bitstring
import traceback


def get_stream_type_from_ps(ps_frame_data:bytes):
    '获取流类型'
    try:
        bit_data = bitstring.BitArray(ps_frame_data)
        print(bit_data)
        # ps流的头 一般都是  0x000001ba
        assert bit_data[:32].uint == 0x000001ba
        # 去掉ps头
        extend_len = bit_data[109:112].uint
        #print(extend_len)
        bit_data =  bit_data[(14*8+extend_len*8):]

        # 去掉 system header
        if bit_data[:32].uint == 0x000001bb:
            # 获取长度
            leng = bit_data[32:32+16]
            bit_data = bit_data[32+16+leng.uint*8:]
        # print(bit_data)

        # 去掉psm
        if bit_data[:32].uint == 0x000001bc:
            # 获取长度
            leng = bit_data[32:32 + 16]
            _program_stream_info_len = bit_data[64:80].uint
            _type = bit_data[80+8*_program_stream_info_len+16:80+8*_program_stream_info_len+16+16]
            if _type.uint == 0x24e0:
                return 'hevc'
            if _type.uint == 0x1be0:
                return 'h264'
            if _type.uint == 0x80e0:
                return 'svac'
            bit_data = bit_data[32 + 16 + leng.uint * 8:]

        # 对于没有psm的直接通过裸流的前缀判断
        if bit_data[:28].uint == 0x000001e:
            pes_packet_len = bit_data[32:32 + 16].uint
            # print(len(bit_data.tobytes()), pes_packet_len)
            pes_data = bit_data[48: 48 + pes_packet_len * 8]
            # print(_data)
            #bit_data = bit_data[48 + pes_packet_len * 8:]
            # print(_data[0:2],_data[8:16].uint,_data[0:2].uint == 0b10)
            # if _data[0:2].uint == 0b10 and _data[8:16].uint == 0x80:
            # print(pes_data[0:2],pes_data[0:16],pes_data[8:16].uint)
            if pes_data[0:2].uint == 0b10:
                # 只对特定格式解析
                #i
                # print("pts",pts, _pts)
                extra_len = pes_data[16:24].uint
                # print('extra_len',extra_len)
                _h26x_bit_data = pes_data[24 + extra_len * 8:]
                #print(_h264_bit_data)
                _h26x_data = _h26x_bit_data.tobytes()
                if _h26x_data.startswith(b"\x00\x00\x00\x01\x02") or _h26x_data.startswith(b"\x00\x00\x00\x00\x01\x02"):
                    return 'hevc'
                else:
                    return 'h264'
    except Exception as e:
        print(str(e))
        pass
    #return 'h264'


def get_raw_stream_from_ps(ps_frame_data:bytes):
    # 从ps 一帧中提取 payload 。得到h264或者hevc的数据 和 pts
    try:
        bit_data = bitstring.BitArray(ps_frame_data)
        # ps流的头 一般都是  0x000001ba
        assert bit_data[:32].uint in (0x000001ba, 0x000001c0)
        # 去掉ps头
        if bit_data[:32].uint == 0x000001ba:
            extend_len = bit_data[109:112].uint
            bit_data = bit_data[(14*8+extend_len*8):]
        # 去掉 system header
        if bit_data[:32].uint == 0x000001bb:
            # 获取长度
            leng = bit_data[32:32+16]
            bit_data = bit_data[32+16+leng.uint*8:]
        # 去掉psm
        if bit_data[:32].uint == 0x000001bc:
            # 获取长度
            leng = bit_data[32:32 + 16]
            #print(leng)
            bit_data = bit_data[32 + 16 + leng.uint * 8:]

        # 如果是pes包
        # pes 包的开头是 0x000001e
        # print("start handle  pes packet....")
        # print("xxx",bit_data)
        h26x_data = b''
        # audio_data = b''
        pts = 0
        while 1:
            # print(bit_data)
            if bit_data[:28].uint == 0x000001e:
                #
                pes_packet_len = bit_data[32:32 + 16].uint
                # print(len(bit_data.tobytes()), pes_packet_len)
                pes_data = bit_data[48: 48+pes_packet_len*8]
                # print(_data)
                bit_data = bit_data[48+pes_packet_len*8:]
                # print(_data[0:2],_data[8:16].uint,_data[0:2].uint == 0b10)
                # if _data[0:2].uint == 0b10 and _data[8:16].uint == 0x80:
                # print(pes_data[0:2],pes_data[0:16],pes_data[8:16].uint)
                # print("pts dts", pes_data[0:2].uint)
                # print("yyy", pes_data[0:2].uint)
                if pes_data[0:2].uint == 0b10:
                    # 只对特定格式解析
                    if pts == 0:
                        _pts = pes_data[24: 64]
                        pts = (_pts[4:7] + _pts[8:16] + _pts[16:23] + _pts[24:32] + _pts[32:39]).uint
                    #print("pts",pts, _pts)
                    extra_len = pes_data[16:24].uint
                    #print('extra_len',extra_len)
                    _h264_bit_data = pes_data[24 + extra_len * 8:]
                    _h264_data = _h264_bit_data.tobytes()
                    #print("h26x",_h264_bit_data)
                    if _h264_data.startswith(b"\x00\x00\x00\x01\x09\xe0"):
                        _h264_data = _h264_data[6:]
                    #print('h264 data',_h264_bit_data)
                    h26x_data += _h264_data

                    #continue
            else:
                # 对其他非视频帧处理
                pes_packet_len = bit_data[32:32 + 16].uint
                pes_data = bit_data[48: 48 + pes_packet_len * 8]
                bit_data = bit_data[48 + pes_packet_len * 8:]

            if not bit_data:
                break
        return h26x_data, pts
    except Exception as e:
        pass


def get_raw_stream_from_multi_ps(multi_ps_frame_data:bytes):
    # 从ps 多帧中提取 payload 。得到h264或者hevc的数据
    res = []
    try:
        bit_data = bitstring.BitArray(multi_ps_frame_data)
        while 1:
            is_keyframe = False
            # ps流的头 一般都是  0x000001ba
            head = bit_data[:32].uint
            assert head in (0x000001ba, 0x000001c0)
            # 去掉ps头
            if head == 0x000001ba:
                extend_len = bit_data[109:112].uint
                bit_data = bit_data[(14*8+extend_len*8):]
                head = bit_data[:32].uint
            # 去掉 system header
            if head == 0x000001bb:
                # 获取长度
                is_keyframe = True
                leng = bit_data[32:32+16]
                bit_data = bit_data[32+16+leng.uint*8:]
                head = bit_data[:32].uint

            # 去掉psm
            if head == 0x000001bc:
                # 获取长度
                leng = bit_data[32:32 + 16]
                # print(leng)
                bit_data = bit_data[32 + 16 + leng.uint * 8:]
                # head = bit_data[:32].uint

            # 如果是pes包
            # pes 包的开头是 0x000001e
            # print("start handle  pes packet....")
            # print("xxx",bit_data)
            h26x_data = b''
            # audio_data = b''
            pts = 0
            while 1:
                if bit_data[:28].uint == 0x000001e:
                    pes_packet_len = bit_data[32:32 + 16].uint
                    # print(len(bit_data.tobytes()), pes_packet_len)
                    pes_data = bit_data[48: 48+pes_packet_len*8]
                    # print(_data)
                    bit_data = bit_data[48+pes_packet_len*8:]
                    # print(_data[0:2],_data[8:16].uint,_data[0:2].uint == 0b10)
                    # if _data[0:2].uint == 0b10 and _data[8:16].uint == 0x80:
                    # print(pes_data[0:2],pes_data[0:16],pes_data[8:16].uint)
                    # print("pts dts", pes_data[0:2].uint)
                    # print("yyy", pes_data[0:2].uint)
                    if pes_data[0:2].uint == 0b10:
                        # 只对特定格式解析
                        if pts == 0:
                            _pts = pes_data[24: 64]
                            pts = (_pts[4:7] + _pts[8:16] + _pts[16:23] + _pts[24:32] + _pts[32:39]).uint
                        # print("pts",pts, _pts)
                        extra_len = pes_data[16:24].uint
                        # print('extra_len',extra_len)
                        _h264_bit_data = pes_data[24 + extra_len * 8:]
                        _h264_data = _h264_bit_data.tobytes()
                        # print("h26x",_h264_bit_data)
                        if _h264_data.startswith(b"\x00\x00\x00\x01\x09\xe0"):
                            _h264_data = _h264_data[6:]
                        # print('h264 data',_h264_bit_data)
                        h26x_data += _h264_data

                        # continue
                else:
                    # 对其他非视频帧处理
                    pes_packet_len = bit_data[32:32 + 16].uint
                    pes_data = bit_data[48: 48 + pes_packet_len * 8]
                    bit_data = bit_data[48 + pes_packet_len * 8:]
                # print(bit_data[:512])
                if not bit_data or bit_data[:32].bytes == b'\x00\x00\x01\xba':
                    break
            if h26x_data:
                res.append((h26x_data, pts, is_keyframe))
            if not bit_data:
                break
        return res
    except Exception as e:
        print(str(e))
        pass

def h264_is_keyframe(h264_content: bytes): # TODO
    # 判断一个H264帧 是否是关键帧
    pass


def hevc_is_keyframe(hevc_content:bytes):  # TODO
    # 判断一个hevc帧 是否是关键帧
    pass


def get_raw_audio_from_data(data:bytes):
    # 从数据中获取原始音频数据
    audio_data = b''
    try:
        bit_data = bitstring.BitArray(data)
        # ps流的头 一般都是  0x000001ba
        assert bit_data[:32].uint in (0x000001ba, 0x000001c0)
        # 去掉ps头
        if bit_data[:32].uint == 0x000001ba:
            extend_len = bit_data[109:112].uint
            bit_data = bit_data[(14 * 8 + extend_len * 8):]
        # 去掉 system header
        if bit_data[:32].uint == 0x000001bb:
            # 获取长度
            leng = bit_data[32:32 + 16]
            bit_data = bit_data[32 + 16 + leng.uint * 8:]
        # 去掉psm
        if bit_data[:32].uint == 0x000001bc:
            # 获取长度
            leng = bit_data[32:32 + 16]
            # print(leng)
            bit_data = bit_data[32 + 16 + leng.uint * 8:]
        while 1:
            if bit_data[:32].uint == 0x000001c0:
                #
                pes_packet_len = bit_data[32:32 + 16].uint
                #print(len(bit_data.tobytes()), pes_packet_len)
                pes_data = bit_data[48: 48+pes_packet_len*8]
                #print(_data)
                bit_data = bit_data[48+pes_packet_len*8:]
                extra_len = pes_data[16:24].uint
                audio_data += pes_data[24 + extra_len * 8:].tobytes()
            else:
                # 对其他非音频帧处理
                pes_packet_len = bit_data[32:32 + 16].uint
                pes_data = bit_data[48: 48 + pes_packet_len * 8]
                bit_data = bit_data[48 + pes_packet_len * 8:]
            if not bit_data:
                break
    except Exception as e:
        pass
        print("exception", str(e))
    return audio_data



if __name__ == '__main__':
    #a = bitstring.BitArray(hex='0x000001ba44019cc9040100fa07feffff000001d2000001e013fa8c80072100673241fffd000000010201d000517f0810bcfe3d084faae25cba3291683c4a5d8613588919515d58ffacdb16415a485bd7b4ff5332e18ff5f151fd289d096884c5447172c190b47f8057545df4f87ac818851623ba6062126c20933aedc2')
    #res = get_stream_type_from_ps(a.tobytes())
    #print(res)
    with open("../testdir/nvr_test.ps", "rb") as f:
        content = f.read()
    res = get_raw_stream_from_multi_ps(content)
    for i,j in res:
        print(len(i),j)
    #raise
    while 1:
        _ = content[4:].find(b"\x00\x00\x01\xba")
        if _>= 0:
            ps_frame = content[:4+_]
            content = content[4+_:]
            print(bitstring.BitArray(ps_frame)[:120], bitstring.BitArray(content)[:120])
            print(len(ps_frame))
        else:
            ps_frame = content
            content = b''
        if ps_frame:
            _1 = get_raw_stream_from_ps(ps_frame)
            if _1:
                print(len(_1[0]),_1[1],bitstring.BitArray(_1[0][-64:]))
                # with open("b.hevc", "ab+") as ff:
                #     ff.write(_1[0])

        if not content:
            break
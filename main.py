# encoding:utf-8
import concurrent.futures
import csv
import hashlib
import logging
import os
import threading
import time
from collections import deque
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

import requests


def logger_in():
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # 创建 MemoryHandler 类的实例，指定日志缓冲区刷新
    mem = logging.handlers.MemoryHandler(capacity=10)
    # 创建一个日志轮转功能
    ro = logging.handlers.RotatingFileHandler(os.getcwd() + "/running.log", maxBytes=2 * 1024 * 1024,
                                              backupCount=10, encoding='utf-8-sig')  # backup-size 100M
    ro.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s || %(name)s || %(levelname)s || %(message)s')
    # 设置为日志轮转
    ro.setFormatter(formatter)

    # 创建终端输出Handler
    sh = logging.StreamHandler()  # 往屏幕上输出
    sh.setFormatter(formatter)  # 设置屏幕上显示的格式

    # # add ro/mem/sh to logger
    logger.addHandler(ro)
    logger.addHandler(mem)
    logger.addHandler(sh)
    return logger


logger = logger_in()


class Stream:

    def __init__(self, save_path, stream_name, src_m3_scheme, src_m3_domain, cdn_m3_url):
        self.save_path = save_path
        self.stream_name = stream_name
        self.src_m3_scheme = src_m3_scheme
        self.src_m3_domain = src_m3_domain
        self.cdn_m3_url = cdn_m3_url

        self.cdn_m3_md5 = None  # 记录m3u8文件的md5
        self.cdn_m3_EXTINFS = deque([])  # 记录EXTINF信息
        self.cdn_ts_urls = deque([])  # 记录cdn_ts url数据

        self.cdn_ts_keys = deque([])
        self.cdn_ts_values = deque([])

        self.src_ts_urls = deque([])
        self.src_ts_delay_dict = {}  # 记录src_ts url数据

        self.ts_url = None
        self.sequence = None
        self.m3_tail = None  # 记录临时处理的extinf值
        self.ts_tag = None  # 标记ts是否进行过更新，1=是；0=否

        self.ts_tt_k = deque([])  # 一种队列用法，可快速操作，性能优于list.insert() + list.append()
        self.ts_tt_v = deque([])  # 记录 ts_md5, ttfb, ttlb

        self.m3_r_b_len = 0
        self.m3_ttfb = 0
        self.m3_ttlb = 0

        self.m_init()

    def m_init(self, ):
        """
        先获取并处理一次m3u8，初始化实例属性得到ts链接；创建记录时延数据的csv文件，header指定抬头
        :return:
        """

        # 初始化self.src_m3_url
        url_p = urlparse(self.cdn_m3_url)
        url_p._replace(scheme=self.src_m3_scheme, netloc=self.src_m3_domain)
        self.src_m3_url = url_p.geturl()

        # 初始化创建文件夹
        self.stream_path = '{0}{1}/'.format(self.save_path, self.stream_name)
        self.src_path = '{0}src/'.format(self.stream_path)
        self.cdn_path = '{0}cdn/'.format(self.stream_path)

        def mkdir(path):
            if not os.path.exists(path):
                os.makedirs(path)

        mkdir(self.stream_path)
        mkdir(self.src_path)
        mkdir(self.cdn_path)

        # 初始化创建表格
        m3_csv_header = ['cdn_sequence', 'cdn_文件md5', 'cdn_文件Bytes', 'cdn_ttfb(s)', 'cdn_ttlb(s)', 'src_sequence',
                         'src_文件md5', 'src_文件Bytes', 'src_ttfb(s)', 'src_ttlb(s)']
        ts_csv_header = ['ts', 'cdn_文件md5', 'cdn_文件Bytes', 'cdn_ttfb(s)', 'cdn_ttlb(s)', 'src_文件md5',
                         'src_文件Bytes', 'src_ttfb(s)', 'src_ttlb(s)']

        m3_csv_path = '{0}{1}/{1}_m3u8_delay_data.csv'.format(self.save_path, self.stream_name)
        ts_csv_path = '{0}{1}/{1}_ts_delay_data.csv'.format(self.save_path, self.stream_name)

        def touch_csv(path, header):
            if not os.path.exists(path):
                # 编码使用
                with open(path, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerows([header])

        touch_csv(m3_csv_path, m3_csv_header)
        touch_csv(ts_csv_path, ts_csv_header)

        # 获取一次m3u8文件，初始化实例，判断url破解是否正确
        self.process_m3u8_cdn()
        if self.cdn_m3_bytes_ttfb == 0:
            logger.error('"m_init" 初始化实例失败，"get_context" 结果为空; url: {}'.format(self.cdn_m3_url))
            exit(1)

    @staticmethod
    def get_context(url: str):
        """
        文档里说本身有3次建联超时情况的充实，故没写重试代码，返回为空也没关系，后续代码都会判断实例属性是否为空才执行
        :param url: 传入url，
        :return: 二进制字节对象，首字节时延
        """
        _MAX_RESPONSE_OK_NUMBER = 206

        _return_tup = None, 0, 0

        s = requests.Session()
        try:
            r = s.get(url, timeout=(1, 1))

        except requests.HTTPError as err:
            logger.error('requests.HTTPError: {} {}'.format(url, err))
            return _return_tup

        except requests.ReadTimeout as err:
            logger.error('requests.ReadTimeout: {} {}'.format(url, err))
            return _return_tup

        except requests.ConnectionError as err:
            logger.error('requests.ConnectionError: {} {}'.format(url, err))
            return _return_tup

        except requests.exceptions.ChunkedEncodingError as err:
            logger.error('requests.ConnectionError: {} {}'.format(url, err))
            return _return_tup

        r_bytes = r.content
        ttfb = r.elapsed.total_seconds()

        if r.status_code > _MAX_RESPONSE_OK_NUMBER:
            logger.error('"get_context" 状态码 = {}'.format(r.status_code))
            return _return_tup
        r_bytes_len = len(r_bytes)
        return r_bytes, r_bytes_len, ttfb

    @staticmethod
    def get_delay(url):
        start_time = time.time()
        r_bytes, r_bytes_len, r_bytes_ttfb = Stream.get_context(url)
        r_bytes_ttlb = time.time() - start_time
        return r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb

    @staticmethod
    def save_file(file_b, file_path):
        """

        :param file_b: 二进制文件
        :param file_path: 要保存的绝对路径推荐 self.save_path+self.stream_name
        :return:
        """

        with open(file_path, 'wb') as f:
            f.write(file_b)

    def process_m3u8_cdn(self, ):
        """
        更新流实例属性，self.ts_url self.sequence self.targetDuration
        判断m3u8文件最后一行的格式，修改self.m3_tail属性，用于生成self.ts_url
        保存m3u8文件
        v1.1: 修改为将所有ts uri保存到 self.m3_EXTINF，ts下载时遍历
        :return:
        """
        r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.cdn_m3_url)
        self.cdn_m3_len = r_bytes_len
        self.cdn_m3_bytes_ttfb = r_bytes_ttfb
        self.cdn_m3_bytes_ttlb = r_bytes_ttlb
        if r_bytes is None:
            logger.error(
                '"process_m3u8_cdn" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(self.cdn_m3_url))
            self.cdn_m3_md5 = 'get为空，检查日志'
            return
        self.cdn_m3_md5 = hashlib.md5(r_bytes).hexdigest()

        cdn_m3u8_lines = r_bytes.decode('utf-8')
        cdn_m3u8_list = cdn_m3u8_lines.splitlines()
        cdn_m3u8_list = deque(cdn_m3u8_list)

        while cdn_m3u8_list:
            line = cdn_m3u8_list.popleft()
            if '#EXT-X-TARGETDURATION' in line:
                self.cdn_m3_targetDuration = line.split(':')[-1]
                continue
            elif '#EXT-X-MEDIA-SEQUENCE' in line:
                self.cdn_m3_sequence = line.split(':')[-1]
                continue
            elif '#EXTINF' in line:
                self.cdn_m3_EXTINFS.append(cdn_m3u8_list.popleft())

        # 保存m3u8文件
        cdn_m3_name_t = time.strftime('%Y_%m_%d_%H_%M_%S_', time.localtime())
        cdn_m3_path = '{0}/{1}{2}.m3u8'.format(self.cdn_path, cdn_m3_name_t, str(self.cdn_m3_sequence))
        Stream.save_file(r_bytes, cdn_m3_path)
        logger.info('{} cdn m3u8 get success：{}'.format(self.stream_name, self.cdn_m3_sequence))

        while self.cdn_m3_EXTINFS:
            cdn_m3_extinf = self.cdn_m3_EXTINFS.popleft()
            cdn_m3_url_p = urlparse(self.cdn_m3_url)
            if 'http' in cdn_m3_extinf:
                self.cdn_ts_urls.append(cdn_m3_extinf)
                return
            elif '/' in cdn_m3_extinf:
                self.cdn_ts_urls.append('{}://{}{}'.format(cdn_m3_url_p.scheme, cdn_m3_url_p.netloc, cdn_m3_extinf))
                return
            else:
                uri_l = cdn_m3_url_p.path.split('/')
                uri_l[-1] = cdn_m3_extinf
                uri = '/'.join(uri_l)
                self.cdn_ts_urls.append('{}://{}{}'.format(cdn_m3_url_p.scheme, cdn_m3_url_p.netloc, uri))

    def process_m3u8_src(self, ):
        r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.cdn_m3_url)
        self.src_m3_len = r_bytes_len
        self.src_m3_bytes_ttfb = r_bytes_ttfb
        self.src_bytes_ttlb = r_bytes_ttlb
        if r_bytes is None:
            logger.error('"process_m3u8" "if r_b is None" ，"get_context" 结果为空; url: {}'.format(self.src_m3_url))
            self.cdn_m3_md5 = 'get为空，检查日志'
            return
        self.src_m3_md5 = hashlib.md5(r_bytes).hexdigest()

        src_m3u8_lines = r_bytes.decode('utf-8')
        src_m3u8_list = src_m3u8_lines.splitlines()
        src_m3u8_list = deque(src_m3u8_list)
        while src_m3u8_list:
            line = src_m3u8_list.popleft()
            if '#EXT-X-MEDIA-SEQUENCE' in line:
                self.src_m3_sequence = line.split(':')[-1]
                break

        # 保存m3u8文件
        src_m3_name_t = time.strftime('%Y_%m_%d_%H_%M_%S_', time.localtime())
        src_m3_path = '{0}/{1}{2}.m3u8'.format(self.src_path, src_m3_name_t, str(self.src_m3_sequence))
        Stream.save_file(r_bytes, src_m3_path)
        logger.info('{} src m3u8 get success：{}'.format(self.stream_name, self.src_m3_sequence))

        # 通过self.cdn_ts_urls构造self.src_ts_urls
        for cdn_ts_url in self.cdn_ts_urls:
            cdn_ts_url_p = urlparse(cdn_ts_url)
            src_ts_url = cdn_ts_url_p._replace(netloc=self.src_m3_domain).geturl()
            self.src_ts_urls.append(src_ts_url)

    def process_ts_cdn(self):
        while self.cdn_ts_urls:
            ts_url = self.cdn_ts_urls.popleft()
            ts_name = os.path.basename(urlparse(ts_url).path).split('/')[-1]
            ts_path = '{0}/{1}'.format(self.cdn_path, ts_name)

            if os.path.exists(ts_path):
                continue

            self.cdn_ts_keys.append(ts_name)

            r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(ts_url)

            if r_bytes is None:
                logger.error(
                    '{} "process_ts_cdn" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(
                        self.stream_name, self.cdn_m3_url))
                ts_md5 = 'get为空，检查日志'
                self.cdn_ts_values.append([ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb])
                return

            ts_md5 = hashlib.md5(r_bytes).hexdigest()
            self.cdn_ts_values.append([ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb])

            # 保存ts文件
            Stream.save_file(r_bytes, ts_path)
            logger.info('{} cdn ts get success：{}'.format(self.stream_name, ts_url))

    def process_ts_src(self):
        while self.src_ts_urls:
            ts_url = self.src_ts_urls.popleft()
            ts_name = os.path.basename(urlparse(ts_url).path).split('/')[-1]
            ts_path = '{0}/{1}'.format(self.src_path, ts_name)

            if os.path.exists(ts_path):
                continue

            self.src_ts_delay_dict[ts_name] = []

            r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(ts_url)

            if r_bytes is None:
                logger.error(
                    '{} "process_ts_cdn" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(
                        self.stream_name, self.cdn_m3_url))
                ts_md5 = 'get为空，检查日志'
                self.src_ts_delay_dict[ts_name] = [ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb]
                return

            ts_md5 = hashlib.md5(r_bytes).hexdigest()
            self.src_ts_delay_dict[ts_name] = [ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb]

            # 保存ts文件
            Stream.save_file(r_bytes, ts_path)
            logger.info('{} src ts get success：{}'.format(self.stream_name, ts_url))

    def threading_m3u8(self, ):
        self_threading = [self.process_m3u8_cdn, self.process_m3u8_src]

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            thread_pool = []
            for fuc in self_threading:
                # 按函数方式创建线程，不能带括号
                future = executor.submit(fuc)
                thread_pool.append(future)

            for future in concurrent.futures.as_completed(thread_pool):  # 并发执行
                future.result()

    def threading_ts(self):
        threading_cdn = threading.Thread(self.process_ts_cdn())
        threading_src = threading.Thread(self.process_ts_src())
        threading_cdn.start()
        threading_src.start()
        threading_cdn.join()
        threading_src.join()


    def get_stream(self):
        while True:
            self.threading_m3u8()
            self.threading_ts()

            # 间隔1个targetDuration时间获取一次
            time.sleep(int(self.cdn_m3_targetDuration))

def main():
    S = Stream('d:/ts/', 'test_stream_1', 'http', '220.161.87.62:8800', 'http://220.161.87.62:8800/hls/0/index.m3u8')
    S.get_stream()
    print('Done')

if __name__ == '__main__':
    main()

# cdn和src的m3u8文件保存都正常

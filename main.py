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
from requests.packages import urllib3
urllib3.disable_warnings()


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

    def __init__(self, save_path, stream_name, src_m3_url, cdn_m3_url):

        self.save_path = save_path
        self.stream_name = stream_name
        self.src_m3_url = src_m3_url
        self.cdn_m3_url = cdn_m3_url
        # process_m3_cdn
        self.cdn_m3_len = 0
        self.cdn_m3_bytes_ttfb = 0
        self.cdn_m3_bytes_ttlb = 0
        self.cdn_m3_md5 = deque([])  # 记录m3u8文件的md5
        self.cdn_m3_sequence = None
        self.cdn_m3_targetDuration = None
        self.cdn_m3_EXTINFS = deque([])  # 记录EXTINF信息
        self.cdn_ts_urls = deque([])  # 记录cdn_ts url数据
        # process_m3u8_src
        self.src_m3_len = 0
        self.src_m3_bytes_ttfb = 0
        self.src_m3_bytes_ttlb = 0
        self.src_m3_md5 = deque([])
        self.src_m3_sequence = ''
        self.src_m3_EXTINFS = deque([])
        self.src_ts_urls = deque([])
        # process_ts_cdn
        self.cdn_ts_keys = deque([])
        self.cdn_ts_values = deque([])
        # process_ts_src
        self.src_ts_delay_dict = {}  # 记录src_ts url数据

        """
        先获取并处理一次m3u8，初始化实例属性得到ts链接；创建记录时延数据的csv文件，header指定抬头
        :return:
        """

        # 初始化创建requests.Session()对象
        self.cdn_session = requests.Session()
        self.src_session = requests.Session()

        # 初始化self.src_m3_url
        context = Stream.get_context(self.src_session, self.src_m3_url)

        if isinstance(context, requests.Response):
            self.src_m3_url = context.headers.get('Location')

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

        self.m3_csv_path = '{0}{1}/{1}_m3u8_delay_data.csv'.format(self.save_path, self.stream_name)
        self.ts_csv_path = '{0}{1}/{1}_ts_delay_data.csv'.format(self.save_path, self.stream_name)

        def touch_csv(path, header):
            if not os.path.exists(path):
                # 编码使用
                with open(path, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerows([header])

        touch_csv(self.m3_csv_path, m3_csv_header)
        touch_csv(self.ts_csv_path, ts_csv_header)

        # 获取一次m3u8文件，初始化实例，判断url破解是否正确
        self.process_m3u8_cdn()
        # 初始化执行一次，先取出来，以免错位
        self.cdn_m3_md5.popleft()
        if self.cdn_m3_bytes_ttfb == 0:
            logger.error('"m_init" 初始化实例失败，"get_context" 结果为空; url: {}'.format(self.cdn_m3_url))
            exit(1)

    @staticmethod
    def get_context(s: object, url: str):
        """
        文档里说本身有3次建联超时情况的充实，故没写重试代码，返回为空也没关系，后续代码都会判断实例属性是否为空才执行
        :param s: 传入requests.Session()对象
        :param url: 传入url，
        :return: 二进制字节对象，对象长度，首字节时延
        """
        _MAX_RESPONSE_OK_NUMBER = 206

        _return_tup = None, 0, 0

        try:
            r = s.get(url, timeout=(1, 3), allow_redirects=False, verify=False)

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

        # 通常如果有重定向都是源站m3u8 url给重定向，这里判断下，直接返回对象，由 process_m3u8_src 去更新self.src_m3_url
        if r.status_code == 302 or r.status_code == 301:
            return r

        if r.status_code > _MAX_RESPONSE_OK_NUMBER:
            logger.error('"get_context" 状态码 = {}'.format(r.status_code))
            return _return_tup

        r_bytes_len = len(r_bytes)
        return r_bytes, r_bytes_len, ttfb

    @staticmethod
    def get_delay(s, url):
        """
        :param s: requests.Session()对象
        :param url: url
        :return: 字节对象，字节长度，本次请求的首字节时延，完整下载时延
        """
        start_time = time.time()
        r_bytes, r_bytes_len, r_bytes_ttfb = Stream.get_context(s, url)
        r_bytes_ttlb = time.time() - start_time
        return r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb

    @staticmethod
    def save_file(file_b, file_path):
        """
        :param file_b: 二进制文件
        :param file_path: 要保存的绝对路径推荐包括文件后缀
        """

        with open(file_path, 'wb') as f:
            f.write(file_b)

    def process_m3u8_cdn(self, ):
        """
        处理self.cdn_m3_url，更新实例属性，后续在self.process_delay_time()时读取
        """
        r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.cdn_session, self.cdn_m3_url)
        self.cdn_m3_len = r_bytes_len
        self.cdn_m3_bytes_ttfb = r_bytes_ttfb
        self.cdn_m3_bytes_ttlb = r_bytes_ttlb
        if r_bytes is None:
            logger.error(
                '"process_m3u8_cdn" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(self.cdn_m3_url))
            self.cdn_m3_md5.append('get为空，检查日志')
            return
        self.cdn_m3_md5.append(hashlib.md5(r_bytes).hexdigest())

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
        logger.info(
            '{} cdn m3u8 get success：{} ; url: {}'.format(self.stream_name, self.cdn_m3_sequence, self.cdn_m3_url))

        # 处理EXTINF信息，生成ts_urls
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
        r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.src_session, self.src_m3_url)
        self.src_m3_len = r_bytes_len
        self.src_m3_bytes_ttfb = r_bytes_ttfb
        self.src_m3_bytes_ttlb = r_bytes_ttlb
        if r_bytes is None:
            logger.error('"process_m3u8" "if r_b is None" ，"get_context" 结果为空; url: {}'.format(self.src_m3_url))
            self.src_m3_md5.append('get为空，检查日志')
            return
        self.src_m3_md5.append(hashlib.md5(r_bytes).hexdigest())

        src_m3u8_lines = r_bytes.decode('utf-8')
        src_m3u8_list = src_m3u8_lines.splitlines()
        src_m3u8_list = deque(src_m3u8_list)
        while src_m3u8_list:
            line = src_m3u8_list.popleft()
            if '#EXT-X-MEDIA-SEQUENCE' in line:
                self.src_m3_sequence = line.split(':')[-1]
                continue
            elif '#EXT-X-MEDIA-SEQUENCE' in line:
                self.src_m3_sequence = line.split(':')[-1]
                continue
            elif '#EXTINF' in line:
                self.src_m3_EXTINFS.append(src_m3u8_list.popleft())

        # 保存m3u8文件
        src_m3_name_t = time.strftime('%Y_%m_%d_%H_%M_%S_', time.localtime())
        src_m3_path = '{0}/{1}{2}.m3u8'.format(self.src_path, src_m3_name_t, str(self.src_m3_sequence))
        Stream.save_file(r_bytes, src_m3_path)
        logger.info(
            '{} src m3u8 get success：{}; url: {}'.format(self.stream_name, self.src_m3_sequence, self.src_m3_url))

        # 处理EXTINF信息，生成self.src_ts_urls
        while self.src_m3_EXTINFS:
            src_m3_extinf = self.src_m3_EXTINFS.popleft()
            src_m3_url_p = urlparse(self.src_m3_url)
            if 'http' in src_m3_extinf:
                self.src_ts_urls.append(src_m3_extinf)
                return
            elif '/' in src_m3_extinf:
                self.src_ts_urls.append('{}://{}{}'.format(src_m3_url_p.scheme, src_m3_url_p.netloc, src_m3_extinf))
                return
            else:
                uri_l = src_m3_url_p.path.split('/')
                uri_l[-1] = src_m3_extinf
                uri = '/'.join(uri_l)
                self.src_ts_urls.append('{}://{}{}'.format(src_m3_url_p.scheme, src_m3_url_p.netloc, uri))

    def process_ts_cdn(self):
        while self.cdn_ts_urls:
            ts_url = self.cdn_ts_urls.popleft()
            ts_name = os.path.basename(urlparse(ts_url).path).split('/')[-1]
            ts_path = '{0}/{1}'.format(self.cdn_path, ts_name)

            if os.path.exists(ts_path):
                continue

            self.cdn_ts_keys.append(ts_name)

            r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.cdn_session, ts_url)

            if r_bytes is None:
                logger.error(
                    '{} "process_ts_cdn" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(
                        self.stream_name, ts_url))
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

            # 由于源站获取的ts可能和cdn有差异，所以这里不做队列，如果在 self.process_delay_time() 中获取源站时延失败(dict.pop())，就给空值
            self.src_ts_delay_dict[ts_name] = []

            r_bytes, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb = Stream.get_delay(self.src_session, ts_url)

            if r_bytes is None:
                logger.error(
                    '{} "process_ts_src" "if r_b is None" ，"Stream.get_delay" 结果为空; url: {}'.format(
                        self.stream_name, ts_url))
                ts_md5 = 'get为空，检查日志'
                self.src_ts_delay_dict[ts_name] = [ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb]
                return

            ts_md5 = hashlib.md5(r_bytes).hexdigest()
            self.src_ts_delay_dict[ts_name] = [ts_md5, r_bytes_len, r_bytes_ttfb, r_bytes_ttlb]

            # 保存ts文件
            Stream.save_file(r_bytes, ts_path)
            logger.info('{} src ts get success：{}'.format(self.stream_name, ts_url))

    def process_delay_time(self):
        # m3u8时延数据
        m3_time_l = []
        # ts时延数据
        ts_time_l = []

        m3_time_l.append([self.cdn_m3_sequence, self.cdn_m3_md5.popleft(), self.cdn_m3_len, self.cdn_m3_bytes_ttfb,
                          self.cdn_m3_bytes_ttlb,
                          self.src_m3_sequence, self.src_m3_md5.popleft(), self.src_m3_len, self.src_m3_bytes_ttfb,
                          self.src_m3_bytes_ttlb])

        # 判断有没有执行过ts，直到队列取完
        while self.cdn_ts_keys:
            keys = self.cdn_ts_keys.popleft()
            cdn_delay_list = self.cdn_ts_values.popleft()
            try:
                src_delay_list = self.src_ts_delay_dict.pop(keys)
            except KeyError:
                src_delay_list = []
            ts_time_l.append([keys] + cdn_delay_list + src_delay_list)

        if m3_time_l:
            with open(self.m3_csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(m3_time_l)

        if ts_time_l:
            with open(self.ts_csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(ts_time_l)

    def threading_m3u8(self, ):
        threading_cdn = threading.Thread(target=self.process_m3u8_cdn(), name='cdn_m3_threading')
        threading_src = threading.Thread(target=self.process_m3u8_src(), name='src_m3_threading')
        threading_cdn.start()
        threading_src.start()
        threading_cdn.join()
        threading_src.join()

    def threading_ts(self):
        threading_cdn = threading.Thread(target=self.process_ts_cdn(), name='cdn_m3_threading')
        threading_src = threading.Thread(target=self.process_ts_src(), name='src_m3_threading')
        threading_cdn.start()
        threading_src.start()
        threading_cdn.join()
        threading_src.join()

    def get_stream(self):
        while True:
            self.threading_m3u8()
            self.threading_ts()
            self.process_delay_time()

            # 间隔1个targetDuration时间获取一次
            time.sleep(int(self.cdn_m3_targetDuration))


def main():
    with open('stream.conf', encoding='utf-8-sig') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            next(f)
            inss = []
            for stream_conf in f:
                stream_conf = stream_conf.strip()
                s_c_l = stream_conf.split()
                save_path = s_c_l[0]
                stream_name = s_c_l[1]
                cdn_m3_urln = s_c_l[2]
                src_m3_url = s_c_l[3]
                inss.append(Stream(save_path, stream_name, src_m3_url, cdn_m3_urln))

            thread_pool = []
            for fuc in inss:
                # 按函数方式创建线程，不能带括号
                future = executor.submit(fuc.get_stream)
                thread_pool.append(future)

            for future in concurrent.futures.as_completed(thread_pool):  # 并发执行
                future.result()

    print('Done')


if __name__ == '__main__':
    main()

# cdn和src的m3u8文件保存都正常

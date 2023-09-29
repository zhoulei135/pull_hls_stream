# encoding: utf-8
import concurrent.futures
import csv
import hashlib
import logging
import os
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
    ro = logging.handlers.RotatingFileHandler(__file__ + ".log", maxBytes=2 * 1024 * 1024,
                                              backupCount=10)  # backup-size 100M
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

    def __init__(self, stream_name, url_m3, save_path):
        self.m3_EXTINF = []
        self.ts_urls = []
        self.save_path = save_path
        self.ts_url = None
        self.targetDuration = None
        self.sequence = None
        self.m3_tail = None  # 记录临时处理的extinf值
        self.ts_tag = None  # 标记ts是否进行过更新，1=是；0=否
        self.stream_name = stream_name
        self.url_m3 = url_m3
        self.url_m3_p = urlparse(url_m3)
        self.ts_tt_k = deque([])  # 一种队列用法，可快速操作，性能优于list.insert() + list.append()
        self.ts_tt_v = deque([])  # 记录 ts_md5, ttfb, ttlb
        self.m3_md5 = None  # 记录m3u8文件的md5
        self.m3_ttfb = 0
        self.m3_ttlb = 0

        self.m_init()

    def m_init(self, ):
        """
        先获取并处理一次m3u8，初始化实例属性得到ts链接；创建记录时延数据的csv文件，header指定抬头
        :return:
        """

        header_ = ['类型', 'sequence或ts_name', '文件md5', 'ttfb', 'ttlb']

        # 创建文件夹
        stream_path = '{0}{1}/'.format(self.save_path, self.stream_name)
        if not os.path.exists(stream_path):
            os.makedirs(stream_path)

        # 创建记录时延数据的文件
        csv_path = '{0}{1}/1_{1}_delay_data.csv'.format(self.save_path, self.stream_name)
        if not os.path.exists(csv_path):
            # 编码使用
            with open('{0}{1}/1_{1}_delay_data.csv'.format(self.save_path, self.stream_name), 'a', newline='',
                      encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows([header_])
        # 获取一次m3u8文件，初始化实例
        self.process_m3u8()
        if self.m3_ttfb == 0:
            logger.error('"m_init" 初始化实例失败，"get_context" 结果为空; url: {}'.format(self.url_m3))
            exit(1)

    @staticmethod
    def get_context(url: str):
        """
        文档里说本身有3次建联超时情况的充实，故没写重试代码，返回为空也没关系，后续代码都会判断实例属性是否为空才执行
        :param url: 传入url，
        :return: 二进制字节对象，首字节时延
        """
        _MAX_RESPONSE_OK_NUMBER = 206

        s = requests.Session()
        try:
            r = s.get(url, timeout=(1, 1))

        except requests.HTTPError as err:
            logger.error('requests.HTTPError: {} {}'.format(url, err))
            return None, 0

        except requests.ReadTimeout as err:
            logger.error('requests.ReadTimeout: {} {}'.format(url, err))
            return None, 0

        except requests.ConnectionError as err:
            logger.error('requests.ConnectionError: {} {}'.format(url, err))
            return None, 0

        r_bytes = r.content
        ttfb = r.elapsed.total_seconds()

        if r.status_code > _MAX_RESPONSE_OK_NUMBER:
            logger.error('"get_context" 状态码>{}'.format(_MAX_RESPONSE_OK_NUMBER))
            return None, 0
        return r_bytes, ttfb

    @staticmethod
    def save_file(file_b, file_path):
        """

        :param file_b: 二进制文件
        :param file_path: 要保存的绝对路径推荐 self.save_path+self.stream_name
        :return:
        """

        with open(file_path, 'wb') as f:
            f.write(file_b)

    def process_m3u8(self, ):
        """
        更新流实例属性，self.ts_url self.sequence self.targetDuration
        判断m3u8文件最后一行的格式，修改self.m3_tail属性，用于生成self.ts_url
        保存m3u8文件
        v1.1: 修改为将所有ts uri保存到 self.m3_EXTINF，ts下载时遍历
        :return:
        """
        start_time = time.time()
        r_b, self.m3_ttfb = self.get_context(self.url_m3)
        self.m3_ttlb = time.time() - start_time

        if r_b is None:
            logger.error('"process_m3u8" "if r_b is None" ，"get_context" 结果为空; url: {}'.format(self.url_m3))
            return

        self.m3_md5 = hashlib.md5(r_b).hexdigest()
        m3u8 = r_b.decode('utf-8')
        m3_l = m3u8.splitlines()

        while m3_l:
            line = m3_l.pop(0)
            if '#EXT-X-TARGETDURATION' in line:
                self.targetDuration = line.split(':')[-1]
                continue
            elif '#EXT-X-MEDIA-SEQUENCE' in line:
                self.sequence = line.split(':')[-1]
                continue
            elif '#EXTINF' in line:
                self.m3_EXTINF.append(m3_l.pop(0))

        m3_name_t = time.strftime('%Y_%m_%d_%H_%M_%S_', time.localtime())
        m3_path = '{}{}/{}{}.m3u8'.format(self.save_path, self.stream_name, m3_name_t, str(self.sequence))
        self.save_file(r_b, m3_path)
        logger.info('{} m3_success：{}'.format(self.stream_name, self.url_m3))

        while self.m3_EXTINF:
            self.m3_tail = self.m3_EXTINF.pop()
            if 'http' in self.m3_tail:
                self.ts_urls.append(self.m3_tail)
                return
            elif '/' in self.m3_tail:
                self.ts_urls.append('{}://{}{}'.format(self.url_m3_p.scheme, self.url_m3_p.netloc, self.m3_tail))
                return
            else:
                uri_l = self.url_m3_p.path.split('/')
                uri_l[-1] = self.m3_tail
                uri = '/'.join(uri_l)
                self.ts_urls.append('{}://{}{}'.format(self.url_m3_p.scheme, self.url_m3_p.netloc, uri))

    def process_ts(self):
        """
        读self.ts_url属性，下载保存ts文件，ttfb, ttlb 修改到属性，process_delay方法处理
        :return:
        """

        while self.ts_urls:
            self.ts_url = self.ts_urls.pop()
            ts_name = os.path.basename(urlparse(self.ts_url).path).split('/')[-1]
            ts_path = '{}{}/{}'.format(self.save_path, self.stream_name, ts_name)
            if not os.path.exists(ts_path):
                self.ts_tt_k.append(ts_name)

                start_time = time.time()
                r_b, ts_ttfb = self.get_context(self.ts_url)
                ts_ttlb = time.time() - start_time

                if r_b is not None:
                    ts_md5 = hashlib.md5(r_b).hexdigest()
                    self.save_file(r_b, ts_path)
                    self.ts_tt_v.append([ts_md5, ts_ttfb, ts_ttlb])

                    logger.info('{} ts_success：{}'.format(self.stream_name, self.ts_url))

                else:
                    logger.error('"process_m3u8" "if r_b is None" ，"get_context" 结果为空; url: {}'.format(self.url_m3))
                    self.ts_tt_v.append(['get为空，检查日志', ts_ttfb, ts_ttlb])
                    return
            # time.sleep(1)

    def process_delay(self):
        """
        每循环执行一次get_stream后调用该方法处理时延数据，追加写入csv
        :return:
        """
        time_l = []

        # 每次循环 m3u8 必有，所以直接执行
        time_l.append(['m3u8', self.sequence, self.m3_md5, self.m3_ttfb, self.m3_ttlb])
        # 判断有没有执行过ts，直到队列取完
        while self.ts_tt_k:
            time_l.append(['ts', self.ts_tt_k.popleft()] + self.ts_tt_v.popleft())

        csv_path = '{0}{1}/1_{1}_delay_data.csv'.format(self.save_path, self.stream_name)

        if time_l:
            with open(csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(time_l)

    def get_stream(self):
        """
        间隔2/3个targetDuration时间获取m3u8文件，判断m3u8文件有更新，则持续拉流
        :return:
        """
        while True:
            self.process_m3u8()
            self.process_ts()
            self.process_delay()

            # 间隔2/3个targetDuration时间获取一次
            time.sleep(int(int(self.targetDuration) / 3))


def main():
    """
    读取配置文件创建实例到列表，再创建线程，再批量执行
    :return:
    """
    with open('stream.conf') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            inss = []
            for stream_conf in f:
                stream_conf = stream_conf.strip()
                s_c_l = stream_conf.split(' ')
                stream_name = s_c_l[0]
                url_m3 = s_c_l[1]
                save_path = s_c_l[2]
                inss.append(Stream(stream_name, url_m3, save_path))

            thread_pool = []
            for fuc in inss:
                # 按函数方式创建线程，不能带括号
                future = executor.submit(fuc.get_stream)
                thread_pool.append(future)

            for future in concurrent.futures.as_completed(thread_pool):  # 并发执行
                future.result()


if __name__ == '__main__':
    main()

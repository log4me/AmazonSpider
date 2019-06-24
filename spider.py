from multiprocessing.dummy import Pool
# from multiprocessing.pool import Pool
import random
from queue import Queue, Full, Empty
import time
import requests
import chardet
import config
import threading
import os
import urllib3;urllib3.disable_warnings()
from bs4 import BeautifulSoup as BS
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', filename='./spider.log')
from enum import Enum

class PARSER_STATUS(Enum):
    NO_TITLE = 0,
    SUCCESS = 1,
    UNKNOWN = 2

class RWLock(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.rcond = threading.Condition(self.lock)
        self.wcond = threading.Condition(self.lock)
        self.read_waiter = 0    # 等待获取读锁的线程数
        self.write_waiter = 0   # 等待获取写锁的线程数
        self.state = 0          # 正数：表示正在读操作的线程数   负数：表示正在写操作的线程数（最多-1）
        self.owners = []        # 正在操作的线程id集合
        self.write_first = True # 默认写优先，False表示读优先

    def write_acquire(self, blocking=True):
        # 获取写锁只有当
        me = threading.get_ident()
        with self.lock:
            while not self._write_acquire(me):
                if not blocking:
                    return False
                self.write_waiter += 1
                self.wcond.wait()
                self.write_waiter -= 1
        return True

    def _write_acquire(self, me):
        # 获取写锁只有当锁没人占用，或者当前线程已经占用
        if self.state == 0 or (self.state < 0 and me in self.owners):
            self.state -= 1
            self.owners.append(me)
            return True
        if self.state > 0 and me in self.owners:
            raise RuntimeError('cannot recursively wrlock a rdlocked lock')
        return False

    def read_acquire(self, blocking=True):
        me = threading.get_ident()
        with self.lock:
            while not self._read_acquire(me):
                if not blocking:
                    return False
                self.read_waiter += 1
                self.rcond.wait()
                self.read_waiter -= 1
        return True

    def _read_acquire(self, me):
        if self.state < 0:
            # 如果锁被写锁占用
            return False

        if not self.write_waiter:
            ok = True
        else:
            ok = me in self.owners
        if ok or not self.write_first:
            self.state += 1
            self.owners.append(me)
            return True
        return False

    def unlock(self):
        me = threading.get_ident()
        with self.lock:
            try:
                self.owners.remove(me)
            except ValueError:
                raise RuntimeError('cannot release un-acquired lock')

            if self.state > 0:
                self.state -= 1
            else:
                self.state += 1
            if not self.state:
                if self.write_waiter and self.write_first:   # 如果有写操作在等待（默认写优先）
                    self.wcond.notify()
                elif self.read_waiter:
                    self.rcond.notify_all()
                elif self.write_waiter:
                    self.wcond.notify()

    def read_release(self):
        self.unlock()

    def write_release(self):
        self.unlock()



ips = []

ip_queue = Queue(config.MAX_IP_QUEUE_SIZE)
proxies_list = {}
proxies_lock = RWLock()

class ProxyGetter(threading.Thread):
    def __init__(self, ip_queue):
        super(ProxyGetter, self).__init__()
        import re
        self.pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,6})'
        self.pattern = re.compile(self.pattern)
        self.setDaemon(True)
        self.ip_queue = ip_queue

    def run(self):
        last_ip = ''
        while True:
            time.sleep(0.5)
            r = requests.get('')
            content = r.content.decode()
            logging.info(f'PROXY GETTER PROCCESS: get proxy info {content} from server api. current proxy queue size:{self.ip_queue.qsize()}')
            matchs = self.pattern.findall(r.content.decode())
            for match in matchs:
                ip = match[0]
                port = match[1]
                proxies = {
                    'http': f'http://{ip}:{port}',
                    'https':f'https://{ip}:{port}'
                }
                if ip != last_ip:
                    last_ip = ip
                    self.ip_queue.put({'proxies': proxies, 'fail_times': 0, 'ip':ip, 'USED_TIMES':0})
                    logging.info(f'PROXY GETTER PROCESS: add {proxies} to proxy queue')


def get_header():
    return {
        "User-Agent": config.USER_AGENTS[random.randint(0, len(config.USER_AGENTS))],
    }

def get_proxy():
    proxies_lock.read_acquire()
    proxies_list_len = len(proxies_list)
    proxies_lock.read_release()
    if proxies_list_len < config.IP_LIST_MIN_LEN:
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}:current proxy number {proxies_list_len} '
                     f'less than {config.IP_LIST_MIN_LEN}, get from queue.')
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get from queue')
        proxy = ip_queue.get()
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: after get from queue')
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get write lock')
        proxies_lock.write_acquire()
        proxies_list[proxy['ip']] = proxy
        proxies_lock.write_release()
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: release write lock')
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: get proxy {proxy} as download proxy.')
        return proxy
    else:
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get read lock')
        proxies_lock.read_acquire()
        proxy_ip = random.sample(proxies_list.keys(), 1)[0]
        proxy = None
        try:
            proxy = proxies_list[proxy_ip]
        except KeyError:
            pass
        proxies_lock.read_release()
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: release read lock')
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: get proxy {proxy} as download proxy.')
        return proxy

def del_proxy(proxy, immediately=False):
    # if proxy fails more than MAX_FAIL_TIMES: remove it from proxy list
    if immediately or proxy['fail_times'] > config.MAX_FAIL_TIME:
        # del from proxy dict
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get write lock')
        proxies_lock.write_acquire()
        try:
            proxies_list.pop(proxy['ip'])
        except KeyError:
            logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: KeyError 9f {proxy["ip"]}')
        except Exception as e:
            logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: unknown Exception {e}')
        proxies_lock.write_release()
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: release write lock')
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: proxy {proxy} fail time too much, '
                     f'del from proxy list.')
    else:
        # increase fail times
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: proxy {proxy} failed, increase fail times.')
        proxy['fail_times'] += 1
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get write lock')
        proxies_lock.write_acquire()
        proxies_list[proxy['ip']] = proxy
        proxies_lock.write_release()
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: release write lock')

# def del_proxy(ip):
#     r = requests.get(f'http://127.0.0.1:8000/delete?ip={ip}')

# def get_proxy():
#     r = requests.get('http://127.0.0.1:8000/?count=1&protocol=0')
#     ip_ports = json.loads(r.text)
#     if len(ip_ports) > 0:
#         ip = ip_ports[0][0]
#         port = ip_ports[0][1]
#         ip = '37.77.135.126'
#         port = 44497
#         proxies = {
#             'http':f'http://{ip}:{port}',
#             'https':f'http://{ip}:{port}'
#         }
#     else:
#         r = requests.get('http://127.0.0.1:8000/?count=1&protocol=1')
#         ip_ports = json.loads(r.text)
#         if len(ip_ports) > 0:
#             ip = ip_ports[0][0]
#             port = ip_ports[0][1]
#             proxies = {
#                 'http': f'https://{ip}:{port}',
#                 'https': f'https://{ip}:{port}'
#             }
#         else:
#             print('No enough proxies')
#     return ip, proxies


def download(url):
    try:
        proxies = get_proxy()
        while proxies['USED_TIMES'] > config.PROXY_MAX_TIMES:
            logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: delete proxy(proxies) for too many uses.')
            del_proxy(proxies, True)
            proxies = get_proxy()
        if not proxies:
            return False
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: downloading {url} by proxy {proxies}{proxies["USED_TIMES"]} /{config.PROXY_MAX_TIMES}')
        r = requests.get(url=url, headers=get_header(), timeout=config.TIMEOUT, proxies=proxies, verify=False)
        detect_length = min(config.CHAR_DETECT_LENGTH, len(r.content))
        r.encoding = chardet.detect(r.content[0:detect_length])['encoding']
        if not r.ok:
            logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: request of {url} response code is not 200, del {proxies}')
            del_proxy(proxies)
            return False
        else:
            return r.text, proxies
    except Exception as e:
        logging.info(f'CRAW PROCESS {threading.currentThread().getNag
        return False


def init_queue(file_list):
    queue = Queue(config.MAX_QUEUE_SIZE)
    for cat_file in file_list:
        with open(cat_file) as f:
            cat = os.path.splitext(os.path.basename(cat_file))[0]
            dir_path = os.path.join(config.SAVE_PATH_ROOT, cat)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            urls = f.readlines()
            for i in range(len(urls)):
                url_info = {'url':urls[i], 'times': 1, 'cat': cat, 'indx': i}
                filepath = os.path.join(config.SAVE_PATH_ROOT, url_info['cat'],
                                        '{}_{}'.format(url_info['cat'], url_info['indx']))
                if not os.path.exists(filepath + '.html'):
                    queue.put({'url':urls[i], 'times': 1, 'cat': cat, 'indx': i})
    return queue

def parse_html(pageSource, url_info):
    try:
        # filePath = '.'
        filepath = os.path.join(config.SAVE_PATH_ROOT, url_info['cat'], '{}_{}'.format(url_info['cat'], url_info['indx']))
        try:
            bsobj = BS(pageSource, "lxml")
        except Exception as e:
            logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: parser {url_info["url"]} failed for {e}')
            return PARSER_STATUS.UNKNOWN

        # 获取一类亚马逊网页中商品的title
        title = ''
        try:
            title = bsobj.find('span', attrs={'id': "productTitle"}).text
            if title is None or title.strip() == '':
                return PARSER_STATUS.NO_TITLE
        except BaseException:
            return PARSER_STATUS.NO_TITLE

        f_html = open(filepath + '.html', "w", encoding="utf-8")
        f_html.write(pageSource)
        f_html.close()
        fileProd = open(filepath + '.txt', 'w', encoding='utf-8')
        fileProd.write("<p_information>\n")
        fileProd.write('<num>A NEW PRODUCTION</num>\n')
        fileProd.write("<title>{}</title>\n".format(title.strip()))


        # 获取另一类亚马逊网页中商品的价格
        try:
            proPrice = bsobj.find('div', attrs={'id': "olp-upd-new"}).text
            fileProd.write("{}\n".format(proPrice))
        except BaseException:
            pass
        # 获取另一类亚马逊网页中商品的价格
        try:
            proPrice = bsobj.find('tr', attrs={'id': "priceblock_ourprice_row"}).text
            fileProd.write("{}\n".format(proPrice))
        except BaseException:
            pass
        # 获取一类亚马逊网页中商品的内存，容量等细节描述
        try:
            proDesp = bsobj.find('div', attrs={'id': "variation_size_name"}).text
            fileProd.write(proDesp + "\n")
        except BaseException:
            pass
        # 获取一类亚马逊网页中商品图片右侧的简要细节描述
        try:
            proDesp = bsobj.find('div', attrs={'id': "feature-bullets"}).text
            fileProd.write("<detail_p>",proDesp + "\n</detail_p>")
        except BaseException:
            pass
        # 获取一类亚马逊网页中商品图片右侧下方的细节对比
        try:
            proDesp = bsobj.find('div',attrs={'id': "universal-product-alert"}).text
            fileProd.write(proDesp + "\n")
        except BaseException:
            pass
        # 获取一类亚马逊网页商品中的Production Description
        try:#bsobj.find('div', attrs={'class': "apm-sidemodule-textright"}).text代替下文
            fromManu = bsobj.find('h3', attrs={'class': "a-spacing-mini"}).text
            fileProd.write(fromManu + "\n")
        except BaseException:
            pass
# 获取    商品中的From the manufacturer信息
        try:
            fromManu = bsobj.findAll('ul', attrs={'class': "a-unordered-list a-vertical"})
            fms = ''
            for fm in fromManu:
                fms = fms + "\n" + fm.text
            fileProd.write(fms + "\n")
        except BaseException:
            pass
        # 获取商品中的From the manufacturer信息
        try:
            fromManu = bsobj.findAll('td', attrs={'class': "apm-top"}) #更详细的每一个信息
            fms = ''
            for fm in fromManu:
                fms = fms + "\n" + fm.text
            fileProd.write(fms+"\n")
        except BaseException:
            pass
        # 获取一类亚马逊网页商品中的Production Description
        try:
            proDes = bsobj.findAll('div', attrs={'id': "productDescription"})
            fms = ''
            for fm in proDes:
                fms = fms + "\n" + fm.text
            fileProd.write(fms + "\n")
        except BaseException:
            pass
        # 获取一类亚马逊网页商品中的Product information纯粹的表格内容
        try:
            proDetail = bsobj.findAll('table', attrs={'id': "productDetails_detailBullets_sections1"})
            prods = ''
            for pro in proDetail:
                if "function" in pro.text or "declarative" in pro.text:
                    continue
                if "count" in pro.text:
                    continue
                prods = prods + "\n" + pro.text
            fileProd.write("<table>", prods + "\n</table>")
        except BaseException:
            pass
        # 获取一类亚马逊网页商品中的Production Description
        try:
            proDes = bsobj.findAll('div', attrs={'id': "tech-specs-desktop"})
            fms = ''
            for fm in proDes:
                fms = fms + "\n" + fm.text
            fileProd.write(fms + "\n")
        except BaseException:
            pass
        try:  # 细节描述
            proInform = bsobj.findAll('div', attrs={'aria-expanded': "true", 'class':'a-expander-content a-expander-extend-content'})[0].text
            fileProd.write(proInform + "\n")
        except BaseException:
            pass
        # 获取一类亚马逊网页商品中的Production Description
        try:
            proDes = bsobj.findAll('div', attrs={'id': "tech-specs-desktop"})
            fms = ''
            for fm in proDes:
                fms = fms + "\n" + fm.text
            fileProd.write(fms + "\n")
        except BaseException:
            pass
        fileProd.write("</p_information>\n")
        fileProd.close()
        return PARSER_STATUS.SUCCESS
    except Exception as e:
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: parser {url_info["url"]} failed for {e}')
        return PARSER_STATUS.UNKNOWN

def process_url(queue, pool):
    try:
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: get from url queue')
        url_info = queue.get(timeout=config.QUEUE_WAIT_TIMEOUT)
        logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: after get from url queue')
    except Empty:
        return
    download_res = download(url_info['url'])
    if not download_res:
        # download failed, retry after a moment
        try:
            logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: failed @ {url_info["times"]}, downloading {url_info["url"]} ')
            if url_info['times'] > config.RETRIE_MAX_TIMES:
                # write to fail list
                queue.task_done()
                return
            url_info['times'] += 1
            queue.put(url_info, True, config.QUEUE_WAIT_TIMEOUT)
            queue.task_done()
            pool.apply_async(process_url, (queue, pool))

        except Full:
            queue.task_done()
            # write to fail list
    else:
        # download success, process url
        html, proxy = download_res
        parser_status = parse_html(html, url_info)
        if parser_status == PARSER_STATUS.NO_TITLE:
            try:
                logging.debug(
                    f'CRAW PROCESS {threading.currentThread().getName()}: delete proxy(proxies) for captcha.')
                del_proxy(proxy, True)
                url_info['times'] += 1
                queue.put(url_info, True, config.QUEUE_WAIT_TIMEOUT)
            except Exception as e:
                logging.debug(f'CRAW PROCESS {threading.currentThread().getName()}: unknown error {e}.')
        queue.task_done()
        logging.info(f'CRAW PROCESS {threading.currentThread().getName()}: success @ {url_info["times"]}, downloading {url_info["url"]} ')


def main():
    ProxyGetter(ip_queue).start()
    pool = Pool(4)
    urlinfo_queue = init_queue(['baby_toys_url.txt'])
    # sleep for proxy queue full
    logging.info('sleep for proxy queue full.')
    time.sleep(60)
    logging.info('sleep over, start crawing.')
    for i in range(urlinfo_queue.qsize()):
        try:
            pool.apply_async(process_url, (urlinfo_queue, pool))
        except Empty:
            continue
    urlinfo_queue.join()
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
    # html = open('baby_toys_url_10.html').read()
    # parse_html(html, '')
    print('Done')

# -*- coding: utf-8 -*-

import ast
import concurrent.futures
import hashlib
import itertools
import json
import logging
import os
import threading
import time
import pytz
from datetime import datetime
#from functools import reduce
from telegram import Bot

import requests
import urllib3

try:
    import win_unicode_console  # pip安装
    win_unicode_console.enable()  # 解决VSCode on Windows的输出异常问题
except:
    pass  # 不装这个包，只要不在VScode下运行也不一定有问题


class wannengError(Exception):  # 练习写个异常？
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class threadsafe_generator():  # 解决多个线程同时抢占一个生成器导致的错误，乖乖排队去吧您们
    def __init__(self, gen):
        self.gen = gen
        self.lock = threading.Lock()

    def __iter__(self):
        return next(self)

    def next(self):
        with self.lock:
            return next(self.gen)

class MapTypeHelper():
    ''' 匹配不同地图的URL路径参数
    Working on : Overviewer, Mapcrafter
    
    '''
    def __init__(self, name):
        self.name = name
    
    #def

    #map_type = MapTypeHelper(self.getMapType)


class counter():
    def __init__(self):
        self.a = {'404': 0, 'Fail': 0, 'Ignore': 0, 'Added': 0,
                  'Update': 0, 'Replace': 0, 'unModded': 0}

    def plus(self, str):
        self.a[str] += 1

    def __str__(self):
        str = ''
        if self.a['Added'] != 0:
            str += 'Added:\t{}\n'.format(self.a['Added'])
        str += 'Update:\t{}\nUnmodded:\t{}\nIgnore:\t{}\n404:\t{}\nFail:\t{}\n'.format(
            self.a['Update'], self.a['unModded'], self.a['Ignore'], self.a['404'], self.a['Fail'])
        if self.a['Replace'] != 0:
            str += 'Replace:\t{}\n'.format(self.a['Replace'])
        return str

class crawler():
    def __init__(self, config, noFetch=False):
        """
        初始化图片抓取器

        从配置文件读取各种路径

        Args: 
            config (dict): See ./config_example.json for detail.
            noFetch (bool): Skip fetchTotalDepth() when set to True. 
                            Save your time when not interacting with overviewer map.                    
        """
        '''文件/路径设置'''
        '''一个正确的链接,overviewer版 https://map.nyaacat.com/kedama/v2_daytime/0/3/3/3/3/3/3/2/3/2/3/1.jpg?c=1510454854'''
        '''mapcrafter版： https://map.nyaacat.com/kedama/v3_daytime/tl/3/2/2/2/2/2/4.jpg'''
        self.map_domain = config['map_domain']  # Overviewer地图地址
        self.map_name = config['map_name']  # 地图名称
        
        self.map_savename = self.map_name if 'map_savename' not in config else config['map_savename']
        
        self.image_folder = 'images/{}'.format(self.map_savename)  # 图块存哪
        self.data_folder = 'data/{}'.format(self.map_savename)  # 更新历史存哪（以后升级数据库？）
        self.log_folder = 'log/{}'.format(self.map_savename)  # 日志文件夹

        os.environ['TZ'] = 'Asia/Shanghai' #保留这行 毕竟在Linux里还会用，能让日志日期正确。
        self.today = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y%m%d')
        self.logger = self.makeLogger()  # 日志记录器
        self.timestamp = str(int(time.time()))  # 请求图块要用，时区无关
        self.logger.debug('Today is set to {}'.format(self.today))

        '''抓取设置'''
        self.map_type = self.getMapType() if noFetch == False else config['latest_renderer'] #渲染器种类
        self.map_rotation = config['map_rotation'] if 'map_rotation' in config else 'tl'
        self.max_threads = config['max_crawl_threads']  # 最大抓图线程数
        # 缩放级别总数
        self.total_depth = config['last_total_depth'] if noFetch == True else self.fetchTotalDepth(
        )
        # 目标图块的缩放级别,从0开始，每扩大观察范围一级-1。
        self.target_depth = config['target_depth']
        # 追踪变迁历史的区域， [((0, -8), 56, 29)] for  v1/v2 on Kedama server
        self.crawl_zones = ast.literal_eval(config['crawl_zones'])

    def getMapType(self):
        """确定地图种类
        
        探测地图站点中，与特定渲染器相关的js文件（名），从而得知它们用了什么渲染器。
        https://map.example.com/kedama/static/js/mapcrafterui.js
        https://ob-mc.net/build/overviewer.js

        Input: none 
        Return: str: 'Overviewer' or 'Mapcrafter' "
        
        """
        renderer_names = {'Mapcrafter' : 'static/js/mapcrafterui.js', 'Overviewer' : 'overviewerConfig.js'}
        errors = 0
        
        for renderer_name in renderer_names:
            URL = '{}/{}'.format(self.map_domain, renderer_names[renderer_name])
            print(URL)
            try:
                r = requests.head(URL, timeout=5)
                if r.status_code == 404:
                    self.logger.debug('The renderer is not {}'.format(renderer_name))
                elif r.status_code == 200:
                    self.logger.info('The renderer is {}.'.format(renderer_name))
                    return renderer_name
                else:#异常处理？
                    raise wannengError(r.status_code)
            except Exception as e:
                self.logger.error('Err {} : {}'.format(errors, e))
                errors += 1
                if errors >= 5:
                    raise e

        #If the code reaches here, no renderer is found, raise an exception.
        raise wannengError("No known renderer found.")
        

    def makeLogger(self):
        """申请并配置抓取器所用到的日志记录器

        若日志文件夹不存在将被创建，向终端输出长度较短的文本，向日志文件写入完整长度的报告

        Todo: 每次抓取摘要：时间，抓取地图，抓取结果统计[，存储配额剩余容量]
        
        Args: None

        Returns: instance of `logging`"""
        logger = logging.getLogger(self.map_savename)
        logger.setLevel(logging.DEBUG)
        log_path = '{}/{}.log'.format(self.log_folder, self.today)  # 文件名
        if not os.path.exists(self.log_folder):  # 初次运行，创建log文件夹
            os.makedirs(self.log_folder)
            print('Made directory\t./'+self.log_folder)
        fh = logging.FileHandler(log_path)
        ch = logging.StreamHandler()
        fh.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
        datefmt_ch = '%H:%M:%S'  # 输出毫秒要改logging的代码，你想清楚就好。
        fmt_fh = '[%(asctime)s]-[%(levelname).1s:%(funcName)-20.15s] %(message)s'
        # 屏幕输出相对简短
        fmt_ch = '[%(asctime)s.%(msecs)03d]-[%(levelname).1s:%(funcName).6s] %(message).60s'
        fh.setFormatter(logging.Formatter(fmt_fh))
        ch.setFormatter(logging.Formatter(fmt_ch, datefmt_ch))
        logger.addHandler(fh), logger.addHandler(ch)
        return logger

    def downloadImage(self, URL):
        """下载给定URL的文件并返回

        别瞎改，改了(1)次又改回来了

        Args: 
            URL (str): URL of the image which is going to be downloaded.

        Returns:
            dict: The response headers of the given URL, the image object.

        Raises:
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
            urllib3.exceptions.ReadTimeoutError
            
            Let the caller function handle these errors."""

        r = requests.get(URL, stream='True', timeout=5)
        if r.status_code == 200:
            img = r.raw.read()
            return {'headers': r.headers, 'image': img}

    def makePath(self, zoneLists, depth):
        """生成给定观察区域的图块路径。

        从左到右处理各列，从上到下生成每列中的path。

        Args: eg. : [((0, -8), 56, 29)] , -3
            zonelists (list of tuple): contains a list of zones that we are watching.
                zones (tuple): (center_X,center_Y), width, height
                    center_X, center_Y (int): The center of a watch zone.
                    width (int): The horizontal image-block numbers at the given zoom depth.
                    height (int) : The vertical image-block numbers at the given zoom depth.
            depth (int): target zoom depth, should be a negative number."""

        for center, width, height in zoneLists:  # 开始对给定的区域**之一**生成坐标
            X_list = [X for X in range(center[0]-width*2**-depth, center[0] +
                                       width*2**-depth) if (X / (2**-depth)) % 2 == 1]
            Y_list = [Y for Y in range(center[1]+height*2**-depth, center[1] -
                                       height*2**-depth, -1) if (Y / (2**-depth)) % 2 == 1]
            for XY in itertools.product(X_list, Y_list):  # 求两个列表的笛卡尔积
                yield self.xy2Path(XY, self.total_depth)

    def xy2Path(self, XY, depth):
        """由图块坐标生成图块路径
        
        Args: Eg. : (12,4) , 4
            XY (int,int): The X and Y coordinates for the project-defined coordinate system. 
            depth (int): The total zoom-levels for the given overviewer map.
        
        Returns: Eg. : '/0/3/3/3/1/2/1/3'
            path(str): Path of the img block.  """

        X = XY[0]
        Y = XY[1]  # 期望坐标值
        table = ['/2', '/0', '/3', '/1']
        val_X = 0
        val_Y = 0  # 本次迭代后的坐标值
        p = depth  # 当前处理的缩放等级
        path = ''  # 返回值的初值
        while (val_X != X) and (val_Y != Y):  # 未迭代到期望坐标点：依次计算横纵坐标下一层是哪块
            p -= 1
            #01|11  |----\  0|1
            #00|10  |----/  2|3
            tmp_X = 0 if val_X > X else 1
            val_X += (2 * tmp_X - 1) * (2 ** p)
            tmp_Y = 0 if val_Y > Y else 1
            val_Y += (2 * tmp_Y - 1) * (2 ** p)
            tmp = tmp_X * 2 + tmp_Y
            path += table[tmp]
        return path

    def path2xy(self, path, depth):
        """由图块路径转项目定义的坐标。
        
        传参时应保证path与给定爬图等级相符，即 path = total_depth + target_depth 
        
        Args: E.g. : '/0/3/3/3/1/2/1/3' , 15
            path (int) : The overviewer img block path to convert.
                P.S.: The '/' in the beginning is needed
            depth (int) : The total zoom-levels for the given overviewer map."""

        in_list = map(int, path.split('/')[1:])
        X, Y = (0, 0)
        table = [1, 3, 0, 2]
        for index, value in enumerate(in_list):
            X += (table[value]//2-0.5)*2**(depth-index)  # 需要整数除
            Y += (table[value] % 2-0.5)*2**(depth-index)
        return(int(X), int(Y))

    '''
    若受网络等影响未获取到值，则整个脚本退出。'''

    def fetchTotalDepth(self):
        """逐层爬取图块，探测给定的Overviewer地图一共多少层

        按照它们生成地图的规则，硬编码取最靠近地图坐标原点右上方的图块，例如 /1 /1/2 /1/2/2 ……(Overviewer版本)
        该函数返回crawler类初始化所需的参数，若发生异常则脚本文件应当退出，而不能向下执行。

        Args: None

        Returns:
            depth (int): The total zoom-levels for the given overviewer map."""

        errors = 0            
        configPaths = {'Overviewer' : 'overviewerConfig.js', 'Mapcrafter' : 'config.js'}
        path = '/1'
        depth = 0
        while True:  # do-while 循环结构的一种改写
            
            URL = {'Overviewer' : '{}/{}{}.jpg?c={}'.format(self.map_domain,self.map_name, path, self.timestamp),
                   'Mapcrafter' : '{}/{}/{}{}.jpg'  .format(self.map_domain,self.map_name, self.map_rotation, path)}

            try:
                print('.', end='', flush=True)  # 只输出，不换行，边爬边输出。
                r = requests.head(URL[self.map_type], timeout=5)
                if r.status_code == 404:
                    break
                elif r.status_code == 200:
                    depth += 1
                    direction_num = {'Overviewer' : '/2', 'Mapcrafter' : '/4'}
                    path = path+direction_num[self.map_type]
                    errors = 0
                else:
                    raise wannengError(r.status_code)
            except Exception as e:
                self.logger.error('Err {} : {}'.format(errors, e))
                errors += 1
                if errors >= 5:
                    raise e
        print()
        self.logger.info("Total zoom depth: {}".format(depth))
        return depth

    def changeImgName(self):
        '''将上一代path命名的文件名和更新记录转换为‘缩放级别_横坐标_纵坐标.jpg’，只用来批量重命名老版本脚本下载的图片
        
        函数已废弃'''
        #先改图片名，再改历史记录
        prevwd = os.getcwd()
        os.chdir(self.image_folder)
        for dir in os.listdir():
            os.chdir(dir)
            time.sleep(3)
            for filename in os.listdir():
                path = filename.split('.')[0].replace('_', '/')
                XY = self.path2xy(path, 11)
                new_file_name = str(self.target_depth) + \
                    '_'+str(XY[0])+'_'+str(XY[1])+'.jpg'
                os.rename(filename, new_file_name)
                print(filename, '-->', new_file_name)
            os.chdir('..')
        os.chdir(prevwd)
        print('changing back to', os.getcwd())

    def changeJsonKey(self):
        '''升级 _更新历史_文件
        
        该函数已废弃'''
        with open(self.data_folder+'/'+'update_history.json', 'r') as f:
            log_buffer = json.load(f)
            new_log_buffer = {}
            for origin_key in log_buffer.keys():
                path = origin_key.split('.')[0].replace('_', '/')
                XY = self.path2xy(path, 11)
                new_key = str(self.target_depth)+'_' + \
                    str(XY[0])+'_'+str(XY[1])+'.jpg'
                new_log_buffer[new_key] = log_buffer[origin_key]
            with open(self.data_folder+'/'+'update_history.json', 'w') as f:  # 写回 图块更新史文件
                json.dump(new_log_buffer, f, indent=2)

    def getImgdir(self, dir):
        """返回保存该地图今日更新了的图块的文件夹地址
        
        保存每张图片前都会检查，若文件夹不存在将被创建。

        Args: Eg. : (.)'/images/v2_daytime'
            dir(str) : Where all images are saved.

        Yields: Eg. : (.)'/images/v2_daytime/20180202/'
            new_dir (str) : Where to save the images being crawled today."""

        new_dir = '{}/{}/'.format(dir, self.today)
        while(True):
            #time.sleep(0.03)  # 不想输出太快
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
                self.logger.info('Made directory\t./{}'.format(new_dir))
            yield new_dir

    '''，，一轮完成而不是先head再get'''       

    def runsDaily(self):
        """每日运行的抓图存图函数，以单个overviewer地图为范围，抓取并更新库中的图片。
        
        第一次运行创建路径和数据文件，全量下/存图片，
        后续每次只下载ETag变动的图片并保存其中SHA1变动的图片(约占前者的1/3?)"""

        bot = Bot(token = "508665684:AAH_vFcSOrXiIuGnVBc-xi0A6kPl1h7WFZc" )

        statistics = counter() # 统计抓图状态
        update_history = {}  # 更新历史
        latest_ETag = {} # 每个区块的最新ETag

        # 读取图块更新史，若文件不存在则连带所述目录一同创建。
        try:
            with open('{}/update_history.json'.format(self.data_folder), 'r') as f:
                update_history = json.load(f)

            with open('{}/latest_ETag.json'.format(self.data_folder), 'r') as f:
               
                    latest_ETag = json.load(f)
        except FileNotFoundError:
                if not os.path.exists(self.data_folder):
                    os.makedirs(self.data_folder)
                    self.logger.info(
                        'Made directory\t./{}'.format(self.data_folder))

        to_crawl = self.makePath(
            self.crawl_zones, self.target_depth)  # 生成要抓取的图片坐标
        save_in = self.getImgdir(self.image_folder)
        save_in = threadsafe_generator(save_in)

        def addNewImg(path, URL, file_name):
            """向文件系统和更新历史记录中添加新图片
            
            Args: 
                URL (str): The url of a specific image.
                file_name (str): What to save the img as.
            Returns:
                ret_msg (str): The log message of the very image."""
            response = self.downloadImage(URL)
            update_history[file_name] = (
                [{'Save_in': save_in.next(), 'ETag': response['headers']['ETag']}])
            with open(save_in.next()+file_name, 'wb') as f:
                f.write(response['image'])
                f.close()
            ret_msg = 'Add\t{}.jpg as {}'.format(path, file_name)
            return ret_msg

        def processBySHA1(URL, response, file_name):
            """下载图块并根据摘要来处理文件
            
            适用于站点最新图片和本地保存的最新图片ETag不同的时候
            
            Args:
                URL (str) : The url of a specific image.
                response : The response of head(url).
                file_name (str): What to save the img as.
            
            Returns:
                ret_msg (str): The log message of the very image."""

            DL_img = self.downloadImage(URL)['image']
            In_Stock_Latest = update_history[file_name][-1]['Save_in'] + file_name
            with open(In_Stock_Latest, 'rb') as Prev_img:
                # SHA1不一致，喻示图片发生了实质性修改
                if hashlib .sha1(Prev_img .read()) .hexdigest() != hashlib .sha1(DL_img) .hexdigest():
                    # 同一天内两次抓到的图片发生了偏差，替换掉本地原来的最新图片和更新记录
                    if update_history[file_name][-1]['Save_in'] == save_in.next():
                        del update_history[file_name][-1]
                        statistics.plus('Replaced')
                        ret_msg = 'Rep\t{}'.format(file_name)
                    else:
                        statistics.plus('Update')
                        ret_msg = 'Upd\t{}'.format(file_name)
                    update_history[file_name].append(
                        {'Save_in': save_in.next(), 'ETag': response.headers['ETag']})
                    with open(save_in.next()+file_name, 'wb') as f:
                            f.write(DL_img)
                            f.close()
                else:
                    # SHA1一致，图片无实质性变化，则忽略该不同
                    statistics.plus('unModded')
                    ret_msg = 'nMOD\t{}'.format(file_name)
                return ret_msg

        def visitPath(path):  #
            """抓取单张图片并对响应进行处理的工人
            
            对每张图片进行最多5次下载尝试，如果还是下不来就放弃这张图片
            
            Args: Eg. : /0/3/3/3/3/3/3/2/3/2/3/1
                path (str): The overviewer img block path to download.

            Returns:
                ret_msg (str): The log message of the very image."""

            URL = '{}/{}{}.jpg?c={}'.format(self.map_domain,
                                            self.map_name, path, self.timestamp)
            tryed_time = 0
            while True:
                visitpath_status = 'none'
                try:
                    r = requests.head(URL, timeout=5)
                    
                    # 404--图块不存在
                    if r.status_code == 404:
                        statistics.plus('404')
                        ret_msg = '404\t{}'.format(path)
                    # 200--OK
                    elif r.status_code == 200:
                        XY = self.path2xy(path, self.total_depth)
                        file_name = '{}_{}_{}.jpg'.format(
                            self.target_depth, XY[0], XY[1])
                        visitpath_status = 'filename set'
                        # 库里无该图--Add
                        if file_name not in update_history:
                            visitpath_status = 'To add img'
                            statistics.plus('Added')
                            ret_msg = addNewImg(path, URL, file_name)
                            latest_ETag[file_name] = {'ETag' : r.headers['ETag']}
                            visitpath_status = 'img added'
                        # 库里有该图片
                        else:
                            # ETag不一致--丢给下一级处理
                            try:
                                if r.headers['ETag'] != latest_ETag[file_name]['ETag']: 
                                    # BUG: 👆latest_etag 中没有部分图块，而update_history里却有。
                                    # 这是由于那些图块均在地图边缘且latest_etag作为独立文件建立较晚，
                                    # 建立后图块就一直没更新了。
                                    # 建议删除update_history中的那些图块并校验两个数据文件中的键一致性。
                                    visitpath_status = 'ETag inconsistent'
                                    ret_msg = processBySHA1(URL, r, file_name)
                                # ETag一致--只出个log
                                else:
                                    visitpath_status = 'ETag consistent'
                                    statistics.plus('Ignore')
                                    ret_msg = 'Ign\t{}'.format(file_name)
                            except KeyError: 
                                # update_history中的部分图块键在latest_etag中没有，是历史遗留问题。
                                # 在这catch掉异常，后面一行代码好添加正确的etag。
                                self.logger.error('{} don\'t show up in latest_ETag but shows in '.format(path))
                                latest_ETag[file_name] = {'ETag' : update_history[file_name][-1]['ETag']}
                                self.logger.error('Copied ETag from update_history to latest_ETag for {}'.format(file_name))
                        latest_ETag[file_name]['ETag'] = r.headers['ETag']
                    return ret_msg
                except (KeyboardInterrupt) as e:
                    raise e
                # 网络遇到问题，重试最多5次
                except Exception as e:
                    self.logger.error(
                        'No.{} for\t{}\t{}'.format(tryed_time, path, e))
                    self.logger.error(visitpath_status)
                    tryed_time += 1
                    if tryed_time >= 5:
                        statistics.plus('Fail')
                        ret_msg = 'Fail\t{}'.format(path)
                        return ret_msg

        # 维护一个抓图线程池
        # Todo: 复用抓图网络连接，减少全程发出的连接数
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            try:
                for msg in executor.map(visitPath, to_crawl):
                    self.logger.warn(
                        msg) if 'Fail' in msg or 'Rep' in msg else self.logger.info(msg)
            except KeyboardInterrupt:
                self.logger.warn('User pressed ctrl+c.')
                self.logger.warn('Will exit when other threads return.')
                return 0

        # 将今天的抓图情况写回更新历史文件
        self.logger.debug('Start dumping json at {}'.format(time.time()))
        with open('{}/update_history.json'.format(self.data_folder), 'w') as f:
            json.dump(update_history, f, indent=2, sort_keys=True)
            self.logger.debug('update_history dumped at {}'.format(time.time()))
        with open('{}/latest_ETag.json'.format(self.data_folder), 'w') as f:
            json.dump(latest_ETag,f,indent=2, sort_keys=True)
            self.logger.debug('latest_ETag dumped at {}'.format(time.time()))
        try:
            bot.send_message(176562893,'Crawl result {} for {} : \n{}'.format(self.today,self.map_name,str(statistics)))
        except Exception :
            self.logger.warning('Telegram bot failed sending {} statistics!'.format(self.map_name))

def main():
    try:
        with open('config.json', 'r+') as f:
            configs = json.load(f)
            for map_name in configs.keys():
                if configs[map_name]['enable_crawl'] == True:
                    cr = crawler(configs[map_name], noFetch=False)
                    cr.runsDaily()
                    if configs[map_name]['last_total_depth'] != cr.total_depth:
                        configs[map_name]['last_total_depth'] = cr.total_depth
                else: 
                    print("skipping map {}".format(map_name))
            f.seek(0)
            json.dump(configs, f, indent=2, sort_keys=True)
            f.truncate()
    except Exception as e:
        print(e)        
        with open('log/errors.txt','a+') as f:
            print(str(e),file = f)
        bot = Bot(token = "508665684:AAH_vFcSOrXiIuGnVBc-xi0A6kPl1h7WFZc" )
        bot.send_message(176562893,'Something went wrong, see logs/errors.txt for detail')



if __name__ == '__main__':
    main()

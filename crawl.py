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
from configs.config import CrawlerConfig as CrConf
from configs.crawl_list import CrawlList as map_list

import requests
import urllib3
import asyncio
import aiohttp
import sqlite3

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

'''
class threadsafe_generator():  # 解决多个线程同时抢占一个生成器导致的错误，乖乖排队去吧您们
    def __init__(self, gen):
        self.gen = gen
        self.lock = threading.Lock()

    def __iter__(self):
        return next(self)

    def next(self):
        with self.lock:
            return next(self.gen)
'''

class MapTypeHelper():
    ''' 匹配不同地图的URL路径参数
    Working on : Overviewer, Mapcrafter
    
    '''
    def __init__(self, name):
        self.name = name
    
    #def

    #map_type = MapTypeHelper(self.UPDConn.getMapType)


class counter():
    def __init__(self):
        self.a = {'404': 0, 'Fail': 0, 'Ignore': 0, 'Added': 0,
                  'Update': 0, 'Replace': 0, 'unModded': 0, 'Cancel': 0}

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
        if self.a['Cancel'] != 0:
            str += 'Cancel:\t{}\n'.format(self.a['Cancel'])
        return str

class crawler():
    # 针对(单张地图,单级缩放)的图块抓取器
    def __init__(self, map_conf, noFetch=False): 
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
        self.map_domain = map_conf.map_domain  # Overviewer地图地址
        self.map_name = map_conf.map_name  # 地图名称
        # sometimes destination map will rename, we can choose a fixed name for them to save.
        self.map_savename = self.map_name if 'map_savename' not in map_conf else map_conf.map_savename
        
        #self.image_folder = '{}/{}/images/{}'.format(CrConf.project_root, CrConf.data_folders, self.map_savename)  # 图块存哪 TODO: deprecate
        self.data_folder  = '{}/{}/data/{}'  .format(CrConf.project_root, CrConf.data_folders, self.map_savename)  # 更新历史存哪（以后升级数据库？）
        self.log_folder  =  '{}/{}/log/{}'   .format(CrConf.project_root, CrConf.data_folders, self.map_savename)  # 日志文件夹

        os.environ['TZ'] = CrConf.timezone #保留这行 毕竟在Linux里还会用，能让日志日期正确。
        self.today = datetime.now(pytz.timezone(CrConf.timezone)).strftime('%Y%m%d')
        self.logger = self.makeLogger()  # 日志记录器
        self.timestamp = str(int(time.time()))  # 请求图块要用，时区无关
        self.logger.debug('Today is set to {}'.format(self.today))

        '''连接“抓取记录”数据库'''
        # 读取图块更新史，若文件不存在则连带所述目录一同创建。
        from util.update_history_DBConn import UpdateHistoryDBConn
        self.UPDConn = UpdateHistoryDBConn(self.logger)
        self.UPDConn.prepare(self.data_folder, self.map_name, self.map_savename, CrConf.storage_type, CrConf.data_folders) 

        '''配置“图库管理器”'''
        from util.file_operator import ImageManager
        self.imgMgr = ImageManager(self.logger, 'local', CrConf.project_root, CrConf.data_folders, self.map_savename)
        self.imgMgr.setDefaultWritePathDate(self.today)

        '''抓取设置'''
        print('pre-dbconn')
        self.map_type = self.probeMapType() if not noFetch else self.UPDConn.getMapLastProbedRenderer() #渲染器种类 # BUG: 此时还未产生数据库连接,是否将UPDConn提前到这里？
        self.map_rotation = map_conf.map_rotation if 'map_rotation' in map_conf else 'tl'
        self.max_crawl_workers = map_conf.max_crawl_workers  # 最大抓图线程数
        # 缩放级别总数 
        self.total_depth = self.UPDConn.getMapLastProbedDepth() if noFetch == True else self.fetchTotalDepth()# BUG: 此时还无DB Conn
        self.logger.info('map type: {}; total depth: {}'.format(self.map_type, self.total_depth))
        # 目标图块的缩放级别,从0开始，每扩大观察范围一级-1。
        self.target_depth = map_conf.target_depth
        # 追踪变迁历史的区域， [((0, -8), 56, 29)] for  v1/v2 on Kedama server
        self.crawl_zones = ast.literal_eval(map_conf.crawl_zones)
        self.dry_run = False # In dry-run mode, neither commit DB nor save image file, while logs are permitted.

    def probeMapType(self):
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
                r = requests.head(URL, timeout = CrConf.crawl_request_timeout)
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
                if errors >= CrConf.crawl_request_retries:
                    raise e

        #If the code reaches here, no renderer is found, raise an exception.
        raise wannengError("No known renderer found.")
        

    def makeLogger(self):
        """申请并配置抓取器所用到的日志记录器

        若日志文件夹不存在将被创建，向终端输出长度较短的文本，向日志文件写入完整长度的报告
        
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
        fmt_ch = '[%(asctime)s.%(msecs)03d]-[{}]-[%(levelname).1s:%(funcName).6s] %(message).60s'.format(self.map_savename)
        fh.setFormatter(logging.Formatter(fmt_fh))
        ch.setFormatter(logging.Formatter(fmt_ch, datefmt_ch))
        logger.addHandler(fh), logger.addHandler(ch)
        return logger


#-----------------File Getter/Putter--------------

    async def downloadImage(self, sess, URL):
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

        '''
        r = requests.get(URL, stream='True', timeout=5)
        if r.status_code == 200:
            img = r.raw.read()
            return {'headers': r.headers, 'image': img}
        '''
        async with sess.get(URL, timeout = 5) as response: 
            if response.status == 200:
                img = await response.read()
            return {'headers': response.headers, 'image': img}

    '''
    def getSavedImage(self, file_name, date): # , DBconn
        #      getSavedImage(self, file_name, date, type = self.getMapStorageType <或fallback什么的>, tolerance = 1 <调用深度<=2 >)
        # An open()able image patch getter that can: 
        # read from local file storage and TODO: object storage. 
        # TODO BUG: what if no DB conn when calling this method?
        img_storage_type = self.UPDConn.getMapStorageType() or CrConf.storage_type  # TODO: UPDConn.get...(): 无数据库连接怎么办  TODO: cache this DB lookup result to avoid excessive lookup.
        img_storage_path = self.UPDConn.getMapDatapath() or CrConf.data_folders # TODO: + TODO: same as above line.
        
        if img_storage_type == 'local':
            with open('{}/{}/images/{}/{}/{}'.format(CrConf.project_root, img_storage_path, self.map_savename, date, file_name), 'rb') as f:
                img = f.read()
            return img # raise any possible error in "local file" mode
        elif img_storage_type == 'S3':
            pass
        # TODO: 如果S3失败则转入Local,如果local失败则raise。
    '''

#--------Kedamadiff-internal-path generator-------

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
            depth (int) : The total zoom-levels for the given overviewer 
                map (total depth)."""

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
        # TODO: migrate to aiohttp?
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

    '''
    def getImgdir(self, dir): # TODO: move to iamge saver TODO: deprecate this.
        """返回保存该地图今日更新了的图块的文件夹地址
        
        保存每张图片前都会检查，若文件夹不存在将被创建。

        Args: Eg. : (.)'../images/v2_daytime'
            dir(str) : Where all images are saved.

        Yields: Eg. : (.)'../images/v2_daytime/20180202/'
            new_dir (str) : Where to save the images being crawled today."""

        new_dir = '{}/{}/'.format(dir, self.today)
        while(True):
            #time.sleep(0.03)  # 不想输出太快
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
                self.logger.info('Made directory\t./{}'.format(new_dir))
            yield new_dir

    #，，一轮完成而不是先head再get     
    '''


    async def processBySHA1(self, sess, URL, response, file_name, coord):
        """下载图块并根据摘要来处理文件
        
        适用于新增图片(Add)及站点最新图片和本地保存的最新图片ETag不同的时候(nMod, upd, rep)
        
        Args:
            sess: aiohttp.ClientSession.
            URL (str) : The url of a specific image.
            response : The response of head(url).
            file_name (str): What to save the img as.
            coord(set(x,y)): x and y coordinate of the image patch.
        
        Returns:
            ret_msg (str): The log message of the very image."""

        #数据库预想：
        #   TODO In_stock_latest 所对应图片的获取方式要变更，
        #   面向数据库和oss做一个文件访问方法，能读/写/获取路径?/创建尚不存在的目录，网络错误则直接raise不用catch
        #
        #   processBySha1 方法名改为 selectiveSaveImg()
        #   新方法：getLatestUpdatePath(), 如filename在DB无记录则返回''。
        #   新方法：addCrawlRecord(), 
        #       在Rep工况下要将已有的相同(日期，文件名)记录置为无效：
        #       新方法： deactivateCrawlRecord()
        
        DL_img = await self.downloadImage(sess, URL)
        DL_img = DL_img['image']
        last_update = self.UPDConn.getLatestUpdateDate(file_name) # NOTE: '20180228', TODO: 改save_in使之能生成此str
        # Determine if the filename inexists in DB
        isAdd = True if last_update == '' else False
        print('isAdd = ', isAdd)
        In_Stock_Latest = last_update + '/' + file_name

        # Calculate SHA1 from saved image patches.
        try:
            #Prev_img = self.getSavedImage(file_name, self.UPDConn.getLatestUpdateDate(file_name)) # BUGFIX: 改成这个文件的last_update 日期
            Prev_img = self.imgMgr.retrieveImage(self.UPDConn.getLatestUpdateDate(file_name), file_name)
            prev_img_SHA1 = hashlib.sha1(Prev_img).hexdigest()
        except FileNotFoundError: # Also happenes when isAdd == True; TODO: change in Object-Storage mode
            self.logger.warning('File\t{} inexist'.format(file_name))
            prev_img_SHA1 = 'G'
        finally:
            DL_img_SHA1 = hashlib.sha1(DL_img).hexdigest()

        # SHA1不一致，喻示图片发生了实质性修改 (Upd, Rep)
        if prev_img_SHA1 != DL_img_SHA1:
            # 同一天内两次抓到的图片发生了偏差，替换掉本地原来的最新图片和更新记录
            # 能否用一条逻辑实现“如果要插入的记录已存在则修改库中记录”？
            
            # (Rep)
            if self.UPDConn.getLatestUpdateDate(file_name) == self.today:                  
                self.UPDConn.deactivateCrawlRecord(file_name, self.today) 
                self.statistics.plus('Replace')
                ret_msg = 'Rep\t{}'.format(file_name)
            # (Upd)
            else:
                if isAdd:
                    self.statistics.plus('Added')
                    ret_msg = 'Add\t{}'.format(file_name)
                else:
                    self.statistics.plus('Update')
                    ret_msg = 'Upd\t{}'.format(file_name)

            #self.update_history[file_name].append(
            #    {'Save_in': self.save_in.next(), 'ETag': response.headers['ETag']})
            self.UPDConn.addCrawlRecord(
                file_name, self.today, response.headers['ETag'], 
                self.target_depth, coord[0], coord[1])
            '''
            with open(self.save_in.next()+file_name, 'wb') as f: # TODO: save_image(), keep aware of dry-run and OSS; TODO: move to save_images
                    f.write(DL_img)
                    f.close()
            '''
            self.imgMgr.saveImage(None, file_name, DL_img)

        else:
            # SHA1一致，图片无实质性变化，则忽略该不同(nMod)
            # file_name is guaranteed to have record.
            self.UPDConn.updateETag(file_name, self.UPDConn.getLatestUpdateDate(file_name),
                            response.headers['ETag'])
            self.statistics.plus('unModded')
            ret_msg = 'nMOD\t{}'.format(file_name)
        return ret_msg

    async def visitPath(self, sess, path):  
        """抓取单张图片并对响应进行处理的工人
        
        对每张图片进行最多5次下载尝试，如果还是下不来就放弃这张图片
        
        Args: Eg. : /0/3/3/3/3/3/3/2/3/2/3/1
            path (str): The overviewer img block path to download.

        Returns:
            ret_msg (str): The log message of the very image."""
        #适应数据库的改造：
        #   replace的数据库处理逻辑要改变，不能删上一天的最新更新记录了。

        URL = '{}/{}{}.jpg?c={}'.format(self.map_domain,
                                        self.map_name, path, self.timestamp)
        tryed_time = 0
        # save_in 本不属于这里，权宜之计，待文件访问方法完工后移除。

        while True:
            visitpath_status = 'none'
            try:
                ret_msg = 'none..'
                async with self.crawlJob_semaphore:
                    async with sess.head(URL, timeout = 5) as response:
                        r = response
                    #r = requests.head(URL, timeout=5)
                    
                    # 404--图块不存在
                    if r.status == 404:
                        self.statistics.plus('404')
                        ret_msg = '404\t{}'.format(path)
                    # 200--OK
                    elif r.status == 200:
                        XY = self.path2xy(path, self.total_depth)
                        file_name = '{}_{}_{}.jpg'.format(
                            self.target_depth, XY[0], XY[1])
                        visitpath_status = 'filename set'
                        '''
                        # 库里无该图--Add
                        if file_name not in self.update_history:
                            visitpath_status = 'To add img'
                            ret_msg = await self.addNewImg(sess, path, URL, file_name)
                            self.latest_ETag[file_name] = {'ETag' : r.headers['ETag']}
                            self.statistics.plus('Added')
                            visitpath_status = 'img added'
                        '''
                        # 库里有该图片
                        #else:
                        # ETag不一致--丢给下一级处理 ((Add,)Upd, nMod, rep)
                        if r.headers['ETag'] != self.UPDConn.getLatestSavedETag(file_name): 
                            visitpath_status = 'ETag inconsistent'
                            ret_msg = await self.processBySHA1(sess, URL, r, file_name, XY)
                        # ETag一致--只出个log
                        else:
                            visitpath_status = 'ETag consistent'
                            self.statistics.plus('Ignore')
                            ret_msg = 'Ign\t{}'.format(file_name)
                        '''
                        except KeyError as e: 
                            # update_history中的部分图块键在latest_etag中没有，是历史遗留问题。
                            # 在这catch掉异常，后面一行代码好添加正确的etag。
                            self.logger.error(e)
                            self.logger.error('{} don\'t show up in latest_ETag but shows in '.format(path))
                            self.latest_ETag[file_name] = {'ETag' : self.update_history[file_name][-1]['ETag']}
                            self.logger.error('Copied ETag from update_history to latest_ETag for {}'.format(file_name))
                        self.latest_ETag[file_name]['ETag'] = r.headers['ETag']
                        '''
                        #BUG：nMod工况下需要更新DB中图块的ETag
                        
                self.logger.warning(ret_msg) if 'Fail' in ret_msg or 'Rep' in ret_msg else self.logger.info(ret_msg)
                return ret_msg
            except (KeyboardInterrupt) as e:
                raise e
            except RuntimeError as e:
                # 任务取消
                if 'Event loop is closed' in str(e):
                    self.statistics.plus('Cancel')
                    ret_msg = 'Cancel\t{}'.format(path)
                    self.logger.debug(ret_msg)
                    return ret_msg
                else:
                    raise e
            # 网络遇到问题，重试最多5次
            # BUG: 异常处理顺序导致部分爬取状态（例如：网络异常爬取报错报错）下的ctrl+c无效。
            except Exception as e:
                #if e is keyboardexception: raise;
                self.logger.error(
                    'No.{} for\t{}({})\t{}'.format(tryed_time, path, self.path2xy(path, self.total_depth), e))
                self.logger.error('at: {}'.format(visitpath_status))
                tryed_time += 1
                if tryed_time >= 5:
                    self.statistics.plus('Fail')
                    ret_msg = 'Fail\t{}'.format(path)
                    self.logger.warning(ret_msg) if 'Fail' in ret_msg or 'Rep' in ret_msg else self.logger.info(ret_msg)
                    return ret_msg
            # using `finally` here will break the 5-time-tolerant `while`-loop.

    async def visitPaths(self, paths):
        self.crawlJob_semaphore = asyncio.Semaphore(self.max_crawl_workers) 
        async with aiohttp.ClientSession() as sess:
            await asyncio.gather(*[self.visitPath(sess, path) for path in paths])
            


    def runsDaily(self):
        """每日运行的抓图存图函数，以单个overviewer地图为范围，抓取并更新库中的图片。
        
        第一次运行创建路径和数据文件，全量下/存图片，
        后续每次只下载ETag变动的图片并保存其中SHA1变动的图片(约占前者的1/3?)"""

        bot = Bot(token = CrConf.telegram_bot_key )

        self.statistics = counter() # 统计抓图状态
        self.update_history = None  # 更新历史
        self.latest_ETag = None # 每个区块的最新ETag


        #save_in = self.getImgdir(self.image_folder) # TODO: deprecate this
        #self.save_in = threadsafe_generator(save_in) #TODO: move

        # Image update history DB read in self.__init__() .

        to_crawl = self.makePath(
            self.crawl_zones, self.target_depth)  # 生成要抓取的图片坐标

        # 维护一个抓图协程(池/队列?),中途退出后下个地图换新队列，原队列雪藏不管。
        # 总之是个 TODO: dirty fix,请把它弄明白并予以改善。
        # 当前问题：ctrl+c后不会扰乱后续地图，但会在所有地图抓取完成后抛出异常: 'loop closed'
        # 调查方向：尝试向gather内传入生成器而非列表[失败，gather不能传生成器]
        # 进一步分析： asyncio工作正常，成功发起了cancel流程并向future内传入了generatorStopException，
        # future(visitpath)也成功catch了,我改了点代码把它们沉默掉并加入统计。
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.visitPaths(to_crawl))
        except KeyboardInterrupt:
            self.logger.warning('User pressed ctrl+c.')
            self.logger.warning('Will exit when other threads return.')
            return 0
        finally:
            # BUG: 如果中途keyboard interrupt的话，DB不应commit而应该rollback.
            self.UPDConn.updateDBDates(self.today, self.total_depth, self.map_type)
            if not self.dry_run:
                self.logger.debug('Start saving DB at {}'.format(time.time()))
                self.UPDConn.commit() # BUG/ISSUE: should we commit/rollback DB on keyboardinterrupt?
            else:
                self.logger.debug('Discarded DB changes in dry-run mode')
            self.UPDConn.close() # BUG: ctrl+c close后，下张地图新初始化的DBConn带有上一个(首个)地图名称。
            # 可能是由于eventLoop在ctrl+c后没有清理掉，请尝试此方向。
            self.logger.info('DB closed')

        try:
            print('Crawl result {} for {} : \n{}'.format(self.today,self.map_name,str(self.statistics)))
            self.logger.info('Crawl result {} for {} : \n{}'.format(self.today,self.map_name,str(self.statistics)))
            self.logger.info('Telegram sending message.')
            bot.send_message(CrConf.telegram_msg_recipient, '{}Crawl result {} for {} : \n{}'.format('[Dev] ' if 'dev' in self.data_folder else '', self.today, self.map_name, str(self.statistics)))
        except Exception :
            self.logger.warning('Telegram bot failed sending {} statistics!'.format(self.map_name))

def main():
    try:
        for map_name, map_conf in map_list.items():
            if map_conf.enable_crawl == True:
                cr = crawler(map_conf, noFetch=False)
                cr.runsDaily()
                del cr
                #if map_conf.last_total_depth != cr.total_depth: -> 丢给数据库处理
                #    map_conf.last_total_depth = cr.total_depth
            else: 
                print("skipping map {}".format(map_name))
        #f.seek(0)
        #json.dump(configs, f, indent=2, sort_keys=True)
        #f.truncate()
    except Exception as e:
        print(e)        
        with open('{}/{}/log/errors.txt'.format(CrConf.project_root, CrConf.data_folders),'a+') as f:
            print(str(e),file = f)
        bot = Bot(token = CrConf.telegram_bot_key )
        bot.send_message(CrConf.telegram_msg_recipient,'Something went wrong, see logs/errors.txt for detail')


if __name__ == '__main__':
    main()

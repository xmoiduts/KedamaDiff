import concurrent.futures
import datetime
import hashlib
import itertools
import json
import logging
import os
import threading
import time
from functools import reduce

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


class threadsafe_generator():
    def __init__(self, gen):
        self.gen = gen
        self.lock = threading.Lock()

    def __iter__(self):
        return next(self)

    def next(self):
        with self.lock:
            return next(self.gen)


class crawler():  # 以后传配置文件
    def __init__(self, test=False):
        '''文件/路径设置'''
        self.map_domain = 'https://map.nyaacat.com/kedama'  # Overviewer地图地址
        self.map_name = 'v1_daytime'  # 地图名称
        self.image_folder = r'images/'+self.map_name  # 图块存哪
        self.data_folder = r'data/'+self.map_name  # 更新历史存哪（以后升级数据库？）
        '''抓取设置'''
        self.max_threads = 16  # 线程数

        if test == True:
            self.total_depth = 15
        else:
            self.total_depth = self.fetchTotalDepth()  # 缩放级别总数
        self.target_depth = -3  # 目标图块的缩放级别,从0开始，每扩大观察范围一级-1。
        # 追踪变迁历史的区域，对于毛线v2是 [((0,0),64,32)]
        self.crawl_zones = [((0, 0), 64, 32)]
        self.timestamp = str(int(time.time()))
        #一个正确的链接 https://map.nyaacat.com/kedama/v2_daytime/0/3/3/3/3/3/3/2/3/2/3/1.jpg?c=1510454854

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

    '''给定[（抓取区域中心点（X,Y坐标），目标缩放深度下横向抓取图块数量，纵向抓取图块数量），……]，目标缩放深度，
    返回一个生成器，按照每列中由上到下，各列从左向右的顺序产出"目标缩放深度_X_Y.jpg"
    * 坐标系中X向右变大，Y向上变大'''

    # func ([ ( (12,4),4,8 ) , …… ] , -2 )
    def makePath(self, zoneLists, depth):
        """生成给定观察区域的图片path。

        从上到下生成每列中的path，从左到右处理各列。

        Args:
            zonelists (list of tuple): contains a list of zones that we are watching.
                zones (tuple): (center_X,center_Y), width, height
                    center_X, center_Y (int): The center of a watch zone.
                    width (int): The horizontal image-block numbers at the given zoom depth.
                    height (int) : The vertical image-block numbers at the given zoom depth."""
        for center, width, height in zoneLists:  # 开始对给定的区域**之一**生成坐标
            X_list = [X for X in range(center[0]-width*2**-depth, center[0] +
                                       width*2**-depth) if (X / (2**-depth)) % 2 == 1]
            Y_list = [Y for Y in range(center[1]+height*2**-depth, center[1] -
                                       height*2**-depth, -1) if (Y / (2**-depth)) % 2 == 1]
            for XY in itertools.product(X_list, Y_list):  # 求两个列表的笛卡尔积
                yield self.xy2Path(XY, self.total_depth)

    '''由图块坐标生成图块路径'''

    def xy2Path(self, XY, depth):  # ((12,4),4)
        #需要坐标和地图总层数来生成完整path
        X = XY[0]
        Y = XY[1]  # 期望坐标值
        table = ['/2', '/0', '/3', '/1']
        val_X = 0
        val_Y = 0  # 本次迭代后的坐标值
        p = depth  # 当前处理的缩放等级
        path = ''  # 返回值的初值
        while (val_X != X) and (val_Y != Y):  # 未迭代到期望坐标点：依次计算横纵坐标下一层是哪块
            p -= 1
            #01|11  0|1
            #00|10  2|3
            tmp_X = 0 if val_X > X else 1
            val_X += (2 * tmp_X - 1) * (2 ** p)
            tmp_Y = 0 if val_Y > Y else 1
            val_Y += (2 * tmp_Y - 1) * (2 ** p)
            tmp = tmp_X * 2 + tmp_Y
            path += table[tmp]
        #print(path)
        return path

    '''由图块路径转坐标，传入的坐标已被筛选，保证是第(total_depth+target_depth)级图片的中心点'''

    def path2xy(self, path, depth):  # '/0/3/3/3/1/2/1/3' 不要丢掉开头的'/'哟;本地图的总层数;
        #print("Inbound:",path)
        in_list = map(int, path.split('/')[1:])
        X, Y = (0, 0)
        table = [1, 3, 0, 2]
        for index, value in enumerate(in_list):
            X += (table[value]//2-0.5)*2**(depth-index)  # 需要整数除
            Y += (table[value] % 2-0.5)*2**(depth-index)
        #print(int(X),int(Y))
        return(int(X), int(Y))

    '''逐层爬取图块，探测当下地图一共多少层,硬编码取地图中心点右上的图块/1 /1/2 /1/2/2 ……
    若受网络等影响未获取到值，则整个脚本退出。'''

    def fetchTotalDepth(self):
        print("Working on", self.map_name,
              "to figure out its zoom levels", end='', flush=True)
        depth = 0
        path = '/1'
        errors = 0
        while True:  # do-while 循环结构的一种改写

            url = self.map_domain+'/'+self.map_name + \
                path+'.jpg?'+str(int(time.time()))
            try:
                print('.', end='', flush=True)  # 只输出，不换行，边爬边输出。
                r = requests.head(url, timeout=5)
                if r.status_code == 404:
                    break
                elif r.status_code == 200:
                    depth += 1
                    path = path+'/2'
                    errors = 0
                else:
                    raise wannengError(r.status_code)
            except Exception as e:
                print(e, errors)
                errors += 1
                if errors >= 5:
                    raise e
        print("\nTotal zoom depth:", depth)
        return depth

    '''将上一代path命名的文件名和更新记录转换为‘缩放级别_横坐标_纵坐标.jpg’，只用来批量重命名老版本脚本下载的图片'''

    def changeImgName(self):
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

    '''升级_更新历史_文件，该函数只用一次'''

    def changeJsonKey(self):
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

    '''永远返回/image_folder/年月日'''
    '''我也不想写这个的，但是直接传image_folder值,后面的executor.map()就只能执行17次'''

    def getImgdir(self, dir):
        today = datetime.datetime.today().strftime('%Y%m%d')
        new_dir = dir+'/'+today+'/'
        while(True):
            time.sleep(0.03)  # 不想输出太快
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
                print('Made directory\t./'+new_dir)
            yield new_dir

    '''每日运行的抓图存图函数，第一次运行创建路径和数据文件，全量下/存图片，后续只下载ETag变动的图片并保存SHA1变动的图片，一轮完成而不是先head再get'''

    def runsDaily(self):
        statistics_count = {'404': 0, 'Fail': 0, 'Ignore': 0,
                            'Added': 0, 'Update': 0, 'Replace': 0}  # 统计抓图状态
        update_history = {}  # 更新历史

        try:  # 读取图块更新史，……
            with open(self.data_folder+'/'+'update_history.json', 'r') as f:
                update_history = json.load(f)
        except FileNotFoundError:  # ……若文件不存在（第一次爬）则创建它所在的目录
                if not os.path.exists(self.data_folder):
                    os.makedirs(self.data_folder)

        to_crawl = self.makePath(
            self.crawl_zones, self.target_depth)  # 生成要抓取的图片坐标
        save_in = self.getImgdir(self.image_folder)
        save_in = threadsafe_generator(save_in)

        def addNewImg(path, URL, file_name):
            response = self.downloadImage(URL)
            update_history[file_name] = (
                [{'Save_in': save_in.next(), 'ETag': response['headers']['ETag']}])
            with open(save_in.next()+file_name, 'wb') as f:
                f.write(response['image'])
                f.close()
            ret_msg = 'Adding\t\t'+path+'.jpg as '+file_name
            return ret_msg

        def processBySHA1(URL, response, file_name):
            DL_img = self.downloadImage(URL)['image']
            In_Stock_Latest = update_history[file_name][-1]['Save_in'] + file_name
            with open(In_Stock_Latest, 'rb') as Prev_img:
                # 【……且SHA1不一致，（喻示图片发生了实质性修改）】
                if hashlib .sha1(Prev_img .read()) .hexdigest() != hashlib .sha1(DL_img) .hexdigest():
                    if update_history[file_name][-1]['Save_in'] == save_in.next() + file_name:
                        #【同一天内两次抓到的图片发生了偏差，用一种dirty hack来处理】
                        del update_history[file_name][-1]
                        ret_msg = 'Replaced\t' + file_name    # warn
                    else:
                        ret_msg = 'Updated\t\t' + file_name    # info
                    update_history[file_name].append(
                        {'Save_in': save_in.next(), 'ETag': response.headers['ETag']})
                    with open(save_in.next()+file_name, 'wb') as f:
                            f.write(DL_img)
                            f.close()
                else:
                    ret_msg = 'Fake-update\t' + file_name  # 【……但SHA1一致，（喻示图片无实质性变化）忽略该不同】
                return ret_msg

        def visitPath(path):  # 抓取单张图片并对响应进行处理的工人
            URL = self.map_domain + '/' + self.map_name + path + '.jpg?c=' + self.timestamp
            tryed_time = 0
            while True:
                try:
                    r = requests.head(URL, timeout=5)  # Head操作
                    if r.status_code == 404:  # 【404，pass】
                        ret_msg = '404\t\t' + path
                    elif r.status_code == 200:
                        XY = self.path2xy(path, self.total_depth)
                        file_name = reduce(
                            lambda a, b: a+b, map(str, [self.target_depth, '_', XY[0], '_', XY[1], '.jpg']))

                        if file_name not in update_history:  # 【库里无该图，Add】
                            ret_msg = addNewImg(path, URL, file_name)
                        else:  # 【库里有该图片，……】
                            # 【……且ETag不一致（喻示图片已更新）……】
                            if r.headers['ETag'] != update_history[file_name][-1]['ETag']:
                                ret_msg = processBySHA1(URL, r, file_name)
                            else:
                                ret_msg = 'Ignored\t\t' + file_name  # 【……但ETag一致（喻示图片未更新）】
                    return ret_msg

                except (
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError,
                        urllib3.exceptions.ReadTimeoutError) as e:
                    print('Error No.', tryed_time, 'for\t', path, e)
                    tryed_time += 1
                    if tryed_time >= 5:
                        ret_msg = 'Abandon\t\t' + path
                        return ret_msg

        # 抓图工人池
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            try:
                for msg in executor.map(visitPath, to_crawl):
                    print(msg)
            except KeyboardInterrupt as e:
                print(e)
                print('Exiting……')
                return 0

        print('start dumping json')
        with open(self.data_folder+'/'+'update_history.json', 'w') as f:  # 更新历史写回文件
            json.dump(update_history, f, indent=2, sort_keys=True)
        print('finish dumping json')


def main():
    cr = crawler()
    cr.runsDaily()


if __name__ == '__main__':
    main()

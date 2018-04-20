import json
import hashlib
import logging
import os
from datetime import datetime
import time
import itertools
from PIL import Image, ImageChops


class joiner():
    def __init__(self):
        os.environ['TZ'] = 'Asia/Shanghai'
        self.map_name = 'v2_daytime'
        self.data_folder = 'data/{}'.format(self.map_name)
        self.product_folder = 'images/joined'
        self.log_folder = 'log/joiner'
        self.logger = self.makeLogger()

    def makeLogger(self):
        """申请并配置拼图器所用到的日志记录器

        若日志文件夹不存在将被创建，向终端输出长度较短的文本，向日志文件写入完整长度的报告

        Todo: ensurePath() 方法，用在crawler/joiner/analyzer的上层调用者
        
        Args: None

        Returns: instance of `logging`"""
        logger = logging.getLogger(self.map_name)
        logger.setLevel(logging.DEBUG)
        log_path = '{}/{}.log'.format(self.log_folder, 'joiner')  # 文件名
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

    def makeImgName(self, zoneLists, depth):
        """生成给定观察区域的图片文件名。

        从左到右处理各列，从上到下生成每列中的path。
        修改自crawler.makePath(),To do: 把这俩函数合起来

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
            for (X, Y) in itertools.product(X_list, Y_list):  # 求两个列表的笛卡尔积
                yield '{}_{}_{}.jpg'.format(depth, X, Y)

    def findPrev(self, file_name, img_hist, date_str):
        '''文件名，文件名为键的值数组，目标日期字符串'''
        target_date = datetime.strptime(date_str, '%Y%m%d')
        for info in reversed(img_hist):
            img_date_str = info['Save_in'].split('/')[-2]
            img_cur_date = datetime.strptime(img_date_str, '%Y%m%d')
            if img_cur_date <= target_date:
                return info['Save_in']
        raise StopIteration

    def makePic1(self, file_name, img_hist, date1, date2):
        '''对指定范围、日期出图
        
        输入多个日期取date1'''
        file_path = self.findPrev(file_name, img_hist, date1)
        self.logger.debug(file_path)
        img = Image.open(file_path + file_name)
        return img

    def makePic2(self, file_name, img_hist, date1, date2):
        '''对指定范围的两个日期输出后的图片，但未变化的图块变淡
        
        ---将输入的图片名分成若干个小格子，相同区域变淡。
        Todo : 改所有的384
        目前只比较两个时间点之间的变化，他们之间若有些日子更新目前却不能考虑。以后加上'''
        tile_size = 8  #单个小格子包含的像素数
        alpha = int(0.75*256) #应该传进来
        try:
            file_path_1, file_path_2 = self.findPrev(
                file_name, img_hist, date1), self.findPrev(file_name, img_hist, date2)
            if (file_path_1 == file_path_2) :#同一张图片/*或两张图片但内容相同*/
                self.logger.debug('{}\tUnchanged'.format(file_name))
                img = Image.open(file_path_2 + file_name)
                bg = Image.new('RGBA', img.size,'white')
                img.putalpha(alpha)
                bg.paste(img,(0,0,img.size[0],img.size[1]),img)
                return bg
            else:  # 图片是变化的
                self.logger.debug(file_name)
                img1, img2 = Image.open(
                    file_path_1 + file_name), Image.open(file_path_2 + file_name)
                tile_width = img1.size[0] // tile_size
                tile_count = tile_width * (img1.size[1]//tile_size)
                bg = Image.new('RGBA', img1.size,'white')
                bbox_gen = [(tile_size*(x % tile_width), tile_size*(x//tile_width), tile_size*(
                    1+x % tile_width), tile_size*(1+x//tile_width)) for x in range(tile_count)]
                for bbox in bbox_gen:
                    tile_1,tile_2 = img1.crop(bbox),img2.crop(bbox)
                    if ImageChops.difference(tile_1, tile_2).getbbox() is None:
                        tile_2.putalpha(alpha)
                        bg.paste(tile_2,bbox,tile_2)
                    else:
                        bg.paste(tile_2,bbox)
                return bg
        except FileNotFoundError:
            return Image.new('RGB', (384,384),'blue')
        
    def findBetween(self,file_name,img_hist,start,end):
        #给定文件名、更新历史、起止时间，返回包含该图块的所有文件夹
        #To do : 这个函数目前还有错误，findPrev不可丢。##改了一下等着去测试吧
        start = datetime.strptime(start, '%Y%m%d')
        end = datetime.strptime(end, '%Y%m%d')
        if end<start:
            start,end = end,start #日期传反也无妨，我们给你纠正过来
        result = []
        for info in reversed(img_hist):#按照日期逆序传入并计算
            img_date_str = info['Save_in'].split('/')[-2]
            current = datetime.strptime(img_date_str, '%Y%m%d')
            if start < current <= end:
                #self.logger.debug('s:{}, c:{}, e:{}'.format(start,current,end))
                result.append(info['Save_in'])  
            elif current <= start:
                result.append(info['Save_in'])  #图块在start日当天的样子可能是当天或上次更新的图片
                result.reverse()# 返回NoneType!!!
                return result #按照日期顺序排列
        raise StopIteration #没找到

    def makeMatrix(self, file_name, img_hist, start, end):
        '''起止时间、区域-->0-1变化矩阵-->高斯模糊、归一化-->取阈值生成0-1矩阵-->暴搜-->返回区域'''
        '''对每个图块生成0-1矩阵'''
        '''统一一下单位：
            在拼图器（和分析器）的坐标系中，由小到大分别可设定为 像素pixel，瓷片tile，图块(img),全图。
            每个瓷片包含整数个像素。每个图块包含整数个瓷片，但图块像素数必须整除瓷片像素数，否则应当抛出异常。
            本程序中，建议将图片(jpg格式)划分成大小为8*8像素的tile，经测试，tile中任何一像素内容发生改变，
            都将影响tile中的其他全部像素的值。因此这个尺寸的tile是图片比对结果的最小单位。
            规定，每瓷片像素数：tile_pixels;本案例中的默认值=8;
            每图块瓷片数：img_tiles;不设默认值，因瓷片大小不定
            每图块像素数：img_pixels;本案例中的默认值=384
            需要注意的是，本坐标系下，二维矩阵个变量的寻址方式是matrix[Y坐标][X坐标]
            性能：v2_daytime，7天，70s，程序总内存60MB'''
        tile_pixels, img_pixels = 8, 384  # Px. #每个tile的边长像素数，图片边长像素数
        img_tiles = img_pixels // tile_pixels  # 每张图片边长多少个tile？
        try:
            folder_list = self.findBetween(
                file_name, img_hist, start, end)  # 所有合格的文件夹名
            if len(folder_list) == 0 or len(folder_list) == 1:  # 没变化，返回全0矩阵(是否妥当)
                self.logger.info(
                    '{} didn\'t change in given period.'.format(file_name))
                matrix = [[0 for x in range(img_tiles)]
                          for y in range(img_tiles)]
                return matrix
            matrix = [[0 for x in range(img_tiles)] for y in range(img_tiles)]
            imgs = [Image.open(folder+file_name) for folder in folder_list]
            #生成每个tile的位置，内循环从左到右，外循环从上到下
            bbox_gen = [(tile_pixels*(x // img_tiles), tile_pixels*(x % img_tiles), tile_pixels*(
                1+x // img_tiles), tile_pixels*(1+x % img_tiles)) for x in range(img_tiles**2)]
            for index, bbox in enumerate(bbox_gen):  # 对于每个tile，遍历图块的全部日期
                for old, new in [(imgs[i].crop(bbox), imgs[i+1].crop(bbox)) for i in range(len(imgs)-1)]:
                    if ImageChops.difference(old, new).getbbox() is None:  # 这两天图片没变
                        pass
                    else:
                        matrix[index % img_tiles][index //
                                                  img_tiles] = 1  # 炼丹出奇迹
                        break  # 后面的图块被短路
            #print(matrix)
            self.logger.info('Diffed: {}'.format(file_name))
            return matrix
        except Exception as e:
            self.logger.warn(e)
            raise e

    def matrixJob(self, core, zone, depth, start_date_str, end_date_str):

        '''分析任务'''
        '''To do : 即使传入多个区域，但只分析第一个。在未来的修改中，放弃用一个list表示多个区域，而将每个区域作为单独的一次作业'''
        '''需要一次变量名统一，关于tile的边长和图块的边长'''
        #初始化二维矩阵：原始数据的0-1矩阵
        tile_pixels,img_pixels = 8 , 384  # 单位：像素
        if img_pixels % tile_pixels != 0:
            raise ValueError
        img_tile_length = img_pixels // tile_pixels #每张图片边长多少个tile？
        matrix_size_X, matrix_size_Y = img_tile_length*zone[0][1], img_tile_length*zone[0][2]
        self.logger.debug('matrix_size_X ,matrix_size_Y = {} , {}'.format(
            matrix_size_X, matrix_size_Y))
        # In Python list: 32.5MB RAM used for v2_daytime, exec time <0.5s
        raw_01matrix = [[0 for x in range(matrix_size_X)]
                        for y in range(matrix_size_Y)]
        self.logger.debug('raw_01matrix allocated')

        #拉取所需文件名和更新历史，内循环从上到下，外循环从左到右
        file_names = self.makeImgName(zone, depth)
        with open('{}/update_history.json'.format(self.data_folder), 'r') as f:
            update_history = json.load(f)

        #生成每个文件名对应区域的二维0-1矩阵，并粘贴到raw矩阵中
        for index , file_name in enumerate(file_names):
            self.logger.info('Now diffing zone {}'.format(file_name))
            try:
                img_hist = update_history[file_name]
                zone_01matrix = core(file_name,img_hist,start_date_str,end_date_str)
                X, Y = ((index//zone[0][2])*img_tile_length, (index % zone[0][2])*img_tile_length) #上述矩阵的左上角在全图0-1矩阵中的坐标（但，X,Y分别是二级下标和一级下标）
                for i in range(len(zone_01matrix)):
                    raw_01matrix[Y+i][X:X+len(zone_01matrix)] = zone_01matrix[i][:]
            except KeyError:
                self.logger.info('{} inexist,skipped'.format(file_name))
                pass #因为初始数组全0，所以不生成图块范围的0-1矩阵也没事
        print('Finished analyzing')
        time.sleep(10)
        #print(raw_01matrix)
        

    def doAJob(self, core, zone, depth, date_str, new_date_str=None):
        '''爬一个区域并按照给定规则生成图片
        
        生成给定区域的全部图片，一个个丢到core内，取回core处理出的图块，进行拼接。
        To do: 模仿concurrent.futures的形式实现多进程操作，利用多核CPU资源。（如果必要的话）
               图像质量降低问题或内存不足问题
        '''
        file_names = self.makeImgName(zone, depth)
        with open('{}/update_history.json'.format(self.data_folder), 'r') as f:
            update_history = json.load(f)  # 文件不存在 异常？
        # 有待观察：对zone我们只取首个元组，传多个区域的迭代处理暂时不写
        canvas = Image.new("RGB", (384*zone[0][1], 384*zone[0][2]))

        for index, file_name in enumerate(file_names):
            try:
                img_hist = update_history[file_name]  # keyError?
                # 丢进去，取出来.虽然核不一样但要喂相同的参数……
                processed_img = core(file_name, img_hist,
                                     date_str, new_date_str)
            except KeyError:  # To do :图片没在库里
                processed_img = Image.new('RGB', (384, 384), 'black')
            X, Y = ((index//zone[0][2]), (index % zone[0][2]))  # 测试输出值啊……
            self.logger.debug('{} loaded,pasting,{},{},{}'.format(file_name,X,Y,processed_img.size))
            canvas.paste(processed_img, (384*X, 384*Y, 384*(X+1), 384*(Y+1)))
            self.logger.debug('{} pasted'.format(file_name))
        if not os.path.exists(self.product_folder):  # 初次运行，创建log文件夹
            os.makedirs(self.product_folder)
        # v2_daytime_-3_zone_20180202[to 20180310].jpg
        result_name = '{}/{}_{}_{}_{}.jpg'.format(self.product_folder,
                                                  self.map_name, depth, zone[0], date_str) if new_date_str == None else '{}/{}_{}_{}_{}_to_{}.jpg'.format(self.product_folder,
                                                                                                                                                          self.map_name, depth, zone[0], date_str, new_date_str)
        self.logger.debug('Start saving {}'.format(result_name))
        canvas.save(result_name, format='JPEG', subsampling=0, quality=100)
        self.logger.info('Finished saving {}'.format(result_name))

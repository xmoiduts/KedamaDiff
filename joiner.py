import json
import hashlib
import logging
import os
from datetime import datetime
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

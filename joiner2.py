import json 
#import hashlib
import logging
import os
from datetime import datetime
import time
import itertools
from PIL import Image,ImageChops
import concurrent.futures

#joiner2.py是对joiner的重写，面向未来可能的特性进行优化

class joiner():
    def __init__(self,depth = -3,alpha = 0.75, mode = 'show',banner = False,tile_pixels = 8,img_pixels=384):
        '''构造方法'''
        self.depth = depth
        self.alpha = alpha
        self.mode = mode #show/diff_a/diff_b
        self.banner = banner
        self.tile_pixels = tile_pixels
        self.img_pixels = img_pixels
        if img_pixels % tile_pixels != 0:
            raise ValueError
        self.img_tiles = self.img_pixels // self.tile_pixels

        '''保佑并发不异常，请勿给concurrent.futures喂函数内定义的函数，此处放置本该在任务方法内的变量。'''
        self.update_history = {}
        self.start = 0 #
        
        self.log_folder = 'log/joiner'
        self.product_folder = 'images/joined'
        os.environ['TZ'] = 'Asia/Shanghai' #调用者写好后，此行删掉。
        self.logger = self.makeLogger()

    def makeLogger(self):
            """申请并配置拼图器所用到的日志记录器，串行。

            若日志文件夹不存在将被创建，向终端输出长度较短的文本，向日志文件写入完整长度的报告

            Todo: ensurePath() 方法，用在crawler/joiner/analyzer的上层调用者
            
            Args: None

            Returns: instance of `logging`"""
            
            logger = logging.getLogger('joiner')
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

    def makeImgName(self, zone, depth):
        """生成给定观察区域的图片文件名。

        从左到右处理各列，从上到下生成每列中的path。
        修改自crawler.makePath(),To do: 把这俩函数丢到util里去

        Args: eg. : [((0, -8), 56, 29)] , -3
            zonelists (list of tuple): contains a list of zones that we are watching.
                zones (tuple): (center_X,center_Y), width, height
                    center_X, center_Y (int): The center of a watch zone.
                    width (int): The horizontal image-block numbers at the given zoom depth.
                    height (int) : The vertical image-block numbers at the given zoom depth.
            depth (int): target zoom depth, should be a negative number."""

        center, width, height = zone[0], zone[1], zone[2]# 对且仅对**一个*区域生成坐标
        X_list = [X for X in range(center[0]-width*2**-depth, center[0] +
                                    width*2**-depth) if (X / (2**-depth)) % 2 == 1]
        Y_list = [Y for Y in range(center[1]+height*2**-depth, center[1] -
                                    height*2**-depth, -1) if (Y / (2**-depth)) % 2 == 1]
        for (X, Y) in itertools.product(X_list, Y_list):  # 求两个列表的笛卡尔积
            yield '{}_{}_{}.jpg'.format(depth, X, Y)

    def findPrev(self, file_name, img_hist, date_str):
        '''文件名，抽出的文件名为键的值数组，目标日期字符串'''
        target_date = datetime.strptime(date_str, '%Y%m%d')
        for info in reversed(img_hist):
            img_date_str = info['Save_in'].split('/')[-2]
            img_cur_date = datetime.strptime(img_date_str, '%Y%m%d')
            if img_cur_date <= target_date:
                return info['Save_in']
        raise StopIteration

    def viewImg(self, file_name, img_hist, date):#准备合并到worker1里？
        '''原makePic1,返回给定位置的图块在给定日期的外观
        如果findPrev抛出了StopIteration，则这里将其继续向上抛出——在给定的日期，这里是一片虚空。'''
        file_path = self.findPrev(file_name, img_hist, date)
        #self.logger.debug(file_path + file_name)
        img = Image.open(file_path + file_name) #fetchImg?
        return img

    
    def worker1(self,file_name):
        '''用于show方法的，可map的工人''' 
        '''其实应该想想如何让一个函数可map。'''
        #time.sleep(0.06)
        try:
            #return Image.new('RGB', (self.img_pixels, self.img_pixels), 'white') if self.findPrev(file_name,self.update_history[file_name],self.start) is not '' else Image.new('RGB', (self.img_pixels, self.img_pixels), 'black')
            return self.viewImg(file_name,self.update_history[file_name],self.start)
        #彼时没有图片的虚空。
        except StopIteration:
            return Image.new('RGB', (self.img_pixels, self.img_pixels), 'black')
        except KeyError:
            return Image.new('RGB', (self.img_pixels, self.img_pixels), 'black')
    
    def joinerjob(self,map_name,zone,start,end='',result_name='') :#只传日期和区域，其他东西初始化就搞好
        '''拼图进程
        地图名 v2_daytime
        区域 ((0, -8), 56, 29)，
        深度在构造方法中，
        起始日期，结束日期 20180909
        自定义文件名 /images/joined/aaaaa.jpg，
        或/images/joined/job1aace046/0001.jpg'''

        #拉取所需文件名和更新历史，内循环从上到下，外循环从左到右
        file_names = self.makeImgName(zone, self.depth)
        self.start = start
        with open('{}/update_history.json'.format('data/{}'.format(map_name)), 'r') as f:
            self.update_history = json.load(f)  # '文件不存在' 异常？
        
        #建立画布和矩阵，方便拼图/diff函数进行计算。
        canvas = Image.new("RGB", (self.img_pixels*zone[1], self.img_pixels*zone[2]))
        if 'diff' in self.mode: #为diff需求建立0-1矩阵
            matrix_width , matrix_height = self.img_tiles * zone[1] , self.img_tiles * zone[2]
            #raw_01matrix = [[0 for x in range(matrix_width)],matrix_height]
            self.logger.info('Made matrix {} * {}'.format(matrix_width, matrix_height))


        with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor: #工人数量
            '''警告，串行执行的代码会影响运行效率
            chunksize是每次发送到worker的任务数量，默认1，调高可“提升性能”
            拼图期间4核8线多进程设定32即可达到理想速度，但还是没有多线程暴力。
            workers和cpu核数相同即可，高了无用。
            '''
            try:
                self.logger.info("mapping workloads")
                em = executor.map(self.worker1,file_names,chunksize=32)
                self.logger.info("mapped")
                self.logger.info("collecting results")
                
                for index,chosen_img in enumerate(em):
                    X, Y = ((index//zone[2]), (index % zone[2])) #待粘贴位置的左上角
                    canvas.paste(chosen_img, (self.img_pixels *X, self.img_pixels *Y, self.img_pixels *(X+1), self.img_pixels *(Y+1)))
                    if not index % 10 : self.logger.info('Processed img {}'.format(index))#子进程日志输出失联，写这个测量时间。
                self.logger.info("collected")
            except KeyboardInterrupt:
                self.logger.warn('User pressed ctrl+c.')
                self.logger.warn('Exit on other workers\' return.')
                return 0
            self.logger.info("Start outputing Img")
            result_name = '{}/{}_{}_{}_{}.jpg'.format(self.product_folder, map_name, self.depth, zone, start) 
            #canvas.thumbnail((2800,1800), Image.ANTIALIAS)
            self.logger.info("Saving Img")
            canvas.save(result_name, format='JPEG', subsampling=0, quality=85)

            
                


def main():
    '''main护身符，保佑concurrent.futures正确运行'''
    jr2 = joiner(mode = 'show')
    #for date in ['20180101','20180115','20180201','20180214','20180301','20180315','20180401','20180415','20180501','20180515','20180601','20180615','20180701','20180715','20180801','20180815','20180901','20180908']:
    jobstart = time.time()
    jr2.joinerjob('v2_daytime',((0, -8), 56, 31),'20180929')
    jobend = time.time()
    print("used {}s".format(jobend - jobstart) )
    #print(date)

if __name__ == '__main__':
    main()
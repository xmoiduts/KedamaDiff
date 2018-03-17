import json
import hashlib
import logging
import os
from datetime import datetime
import itertools
from PIL import Image

class joiner():
    def __init__(self):
        self.map_name = 'v2_daytime'
        self.data_folder = 'data/{}'.format(self.map_name)
        self.product_folder = 'images/joined'
    
    def makeImgName(self,zoneLists,depth):
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
            for (X,Y) in itertools.product(X_list, Y_list):  # 求两个列表的笛卡尔积
                yield '{}_{}_{}.jpg'.format(depth, X, Y)

    def findPrev(self,file_name,img_hist,date_str):
        '''文件名，文件名为键的值数组，目标日期字符串'''
        target_date = datetime.strptime(date_str,'%Y%m%d')
        for info in reversed(img_hist):
            img_date_str = info['Save_in'].split('/')[-2]
            img_cur_date = datetime.strptime(img_date_str,'%Y%m%d')
            if img_cur_date <= target_date:
                return info['Save_in']
        raise StopIteration
        


    def makePic1(self,file_name,img_hist,date1,date2):
        '''对指定范围、日期出图
        
        输入多个日期取date1'''
        file_path = self.findPrev(file_name,img_hist,date1)
        print (file_path)
        img = Image.open(file_path + file_name)
        return img
    
    def makePic2():
        '''对指定范围的两个日期输出较新者的图片，其中未变化的图块变淡'''
        pass

    def doAJob(self, core, zone,depth ,date_str,new_date_str = None):
        '''爬一个区域并按照给定规则生成图片
        
        生成给定区域的全部图片，一个个丢到core内，取回core处理出的图块，进行拼接。
        To do: 模仿concurrent.futures的形式实现多进程操作，利用多核CPU资源。（如果必要的话）
               图像质量降低问题或内存不足问题
        '''
        file_names = self.makeImgName(zone,depth)
        with open('{}/update_history.json'.format(self.data_folder), 'r') as f:
            update_history = json.load(f)  # 文件不存在 异常？
        canvas = Image.new("RGB", (384*zone[0][1], 384*zone[0][2]))  # 有待观察：对zone我们只取首个元组，传多个区域的迭代处理暂时不写

        for index, file_name in enumerate(file_names):
            try:
                img_hist = update_history[file_name]  # keyError?
                processed_img = core(file_name,img_hist,date_str,new_date_str)  # 丢进去，取出来.虽然核不一样但要喂相同的参数……
            except KeyError:
                processed_img = Image.new('RGB',(384,384),'black')
            X, Y = ((index//zone[0][2]), (index % zone[0][2]))  # 测试输出值啊……
            canvas.paste(processed_img, (384*X, 384*Y, 384*(X+1), 384*(Y+1)))
        if not os.path.exists(self.product_folder):  # 初次运行，创建log文件夹
            os.makedirs(self.product_folder)
        canvas.save('{}/{}_{}_{}joined.jpg'.format(self.product_folder,self.map_name,depth,zone[0]),format='JPEG', subsampling=0, quality=100)

    
            
            


    

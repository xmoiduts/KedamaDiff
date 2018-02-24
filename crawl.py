import requests,os
import time,datetime
import concurrent.futures
import ast,json
import hashlib
import logging
import itertools
from functools import reduce
import win_unicode_console
win_unicode_console.enable()    #解决VSCode的输出异常问题

class wannengError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class crawler(): #以后传配置文件
    def __init__ (self,test=False):
        '''文件/路径设置'''
        self.map_domain='https://map.nyaacat.com/kedama'    #Overviewer地图地址
        self.map_name='v2_daytime'                          #地图名称
        self.image_folder=r'images/'+self.map_name          #图块存哪
        self.data_folder =r'data/'+self.map_name            #更新历史存哪（以后升级数据库？）
        '''抓取设置'''
        self.max_threads=16                                 #线程数

        if test == True:
            self.total_depth = 15
        else:
            self.total_depth= self.fetchTotalDepth()        #缩放级别总数
        self.target_depth= -3                               #目标图块的缩放级别,从0开始，每扩大观察范围一级-1。
        self.crawl_zones=[((0,0),4,32)]
        self.timestamp = str(int(time.time()))
        # https://map.nyaacat.com/kedama/v2_daytime/0/3/3/3/3/3/3/2/3/2/3/1.jpg?c=1510454854  

    '''输入图块链接，返回              错误处理应该改进吧     
    def teaseImage(self,URL):
        r=requests.head(URL,timeout=5) #只取响应头而不下载响应体
        return r.headers
        #print(r.headers['Last-Modified'],r.headers['Content-Length']) #404-"KeyError"error.
    '''

    '''下载给定URL的文件并返回。'''
    def downloadImage(self,URL):
        r = requests.get(URL,stream = 'True')
        if r.status_code == 200:
            img = r.raw.read()
            return {'headers' : r.headers , 'image' : img}



    '''输入[多个可采集区域的前缀]和采集深度，生成['pic_name','文件名']。'''
    def makePicName(self,prefixes,depth):
        for i in range(len(prefixes)):                                          #对于每个给定的采集区域
            prefix = prefixes[i].split('/')[1:]                                 #求出其自身的深度
            exp = depth - len(prefix)                                           #并计算元组深度
            for j in range(4**exp):                                             #Example: 4^(6-3)=64，Eg.:34
                to_split  = '0'*(2*exp-len(bin(j)[2:]))+bin(j)[2:]              #000000100010
                split_ed  = [to_split[k:k+2] for k in range(0,len(to_split),2)] #00 00 00 10 00 10
                quaternary= [str(int(i,2)) for i in split_ed]                   #0 0 0 2 0 2
                yield(['/'+'/'.join(prefix+quaternary)+'.jpg','_'+'_'.join(prefix+quaternary)+'.jpg'])
        return

    '''给定[（抓取区域中心点（X,Y坐标），目标缩放深度下横向抓取图块数量，纵向抓取图块数量），……]，目标缩放深度，
    返回一个生成器，按照每列中由上到下，各列从左向右的顺序产出"目标缩放深度_X_Y.jpg"
    * 坐标系中X向右变大，Y向上变大'''
    def makePicXY(self,zoneLists,target_depth): #func ([ ( (12,4),4,8 ) , …… ] , -2 )
        for center,width,height in zoneLists:   #开始对给定的区域**之一**生成坐标
            X_list= [X for X in range(center[0]-width*2**-target_depth ,center[0]+width*2**-target_depth ) if (X / (2**-target_depth)) %2 == 1]
            Y_list= [Y for Y in range(center[1]+height*2**-target_depth,center[1]-height*2**-target_depth,-1) if (Y / (2**-target_depth)) %2 == 1]
            for XY in itertools.product(X_list,Y_list): #求两个列表的笛卡尔积
                yield self.xy2Path(XY,self.total_depth)

    '''由图块坐标生成图块路径'''
    def xy2Path(self,XY,depth):#((12,4),4)
        #需要坐标和地图总层数来生成完整path
        X = XY[0]  ; Y=XY[1]        #期望坐标值
        table=['/2','/0','/3','/1']
        val_X = 0  ; val_Y = 0      #本次迭代后的坐标值
        p = depth                   #当前处理的缩放等级
        path = ''                   #返回值的初值
        while (val_X != X) and (val_Y != Y):    #未迭代到期望坐标点：依次计算横纵坐标下一层是哪块
            p -= 1
            #01|11  0|1
            #00|10  2|3
            tmp_X = 0 if val_X > X else 1
            val_X += (2 * tmp_X - 1) * (2 ** p)
            tmp_Y = 0 if val_Y > Y else 1
            val_Y += (2 * tmp_Y - 1) * (2 ** p)
            tmp = tmp_X *2 + tmp_Y
            path += table[tmp]    
        #print(path)  
        return path

    '''由图块路径转坐标，传入的坐标已被筛选，保证是第(total_depth+target_depth)级图片的中心点'''
    def path2xy(self,path,depth):   #'/0/3/3/3/1/2/1/3' 不要丢掉开头的'/'哟;本地图的总层数;
        #print("Inbound:",path)
        in_list = map(int,path.split('/')[1:])
        X,Y=(0,0)
        table=[1,3,0,2]
        for index,value in enumerate(in_list):
            X += (table[value]//2-0.5)*2**(depth-index) #需要整数除
            Y += (table[value]% 2-0.5)*2**(depth-index)
        #print(int(X),int(Y))
        return(int(X),int(Y))


    '''逐层爬取图块，探测当下地图一共多少层,硬编码取地图中心点右上的图块/1 /1/2 /1/2/2 ……
    若受网络等影响未获取到值，则整个脚本退出。'''
    def fetchTotalDepth(self):
        print("Working on", self.map_name ,"to figure out its zoom levels",end='',flush=True)
        depth=0
        path='/1'
        errors=0
        while True:# do-while 循环结构的一种改写
            
            url=self.map_domain+'/'+self.map_name+path+'.jpg?'+str(int(time.time()))
            try:
                print('.',end='',flush=True) #只输出，不换行，边爬边输出。
                r= requests.head(url,timeout=5)
                if r.status_code==404:
                    break
                elif r.status_code==200:
                    depth += 1
                    path=path+'/2'
                    errors=0
                else:
                    raise wannengError(r.status_code)
            except Exception as e:
                print(e)
                errors += 1
                if errors >= 3:
                    raise e
        print("\nTotal zoom depth:",depth)
        return depth

    '''将上一代path命名的文件名和更新记录转换为‘缩放级别_横坐标_纵坐标.jpg’，只会用到一次'''
    def changeImgName(self):
        #先改图片名，再改历史记录
        prevwd = os.getcwd()
        os.chdir(self.image_folder)
        for dir in os.listdir():
            os.chdir(dir)
            time.sleep(3)
            for filename in os.listdir():
                path = filename.split('.')[0].replace('_','/')
                XY = self.path2xy(path,11)
                new_file_name = str(self.target_depth)+'_'+str(XY[0])+'_'+str(XY[1])+'.jpg'
                os.rename(filename,new_file_name)
                print(filename,'-->',new_file_name)
            os.chdir('..')
        os.chdir(prevwd)
        print('changing back to',os.getcwd())

    '''升级更新历史文件，该函数只用一次'''
    def changeJsonKey(self):
        with open(self.data_folder+'/'+'update_history.json','r') as f:
            log_buffer=json.load(f)
            new_log_buffer = {}
            for origin_key in log_buffer.keys():
                path = origin_key.split('.')[0].replace('_','/')
                XY = self.path2xy(path,11)
                new_key = str(self.target_depth)+'_'+str(XY[0])+'_'+str(XY[1])+'.jpg'
                new_log_buffer [new_key] = log_buffer[origin_key]
            with open(self.data_folder+'/'+'update_history.json','w') as f:#写回 图块更新史文件
                json.dump(new_log_buffer,f,indent=2)

    '''抓图线程
    def dealWithPicurl(self,pic_tuple,save_to,download=False):
        url=self.map_domain+'/'+self.map_name+pic_tuple[0]+'?'+str(int(time.time()))
        filename=pic_tuple[1]
        if download==True: 
            img = self.downloadImage(url)
            return img,filename #fail的异常丢出去
        else:
            try:
                rh=self.teaseImage(url)
                return({'Path':pic_tuple,'Filename':filename,'Lastmod':rh['Last-Modified'],'ETag':rh['ETag']})
            except KeyError:
                return({'Filename':filename,'ERROR':'404'})
            except requests.exceptions.ConnectionError:
                return({'Filename':filename,'ERROR':'Fail'})
            except requests.exceptions.ReadTimeout:
                return({'Filename':filename,'ERROR':'Fail'})
    '''

    '''永远返回/image_folder/年月日'''
    '''我也不想写这个的，但是直接传image_folder值,后面的executor.map()就只能执行17次'''
    def getImgdir(self,dir):
        today=datetime.datetime.today().strftime('%Y%m%d')
        new_dir=dir+'/'+today+'/'
        while(True):
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
                print('Made directory\t./'+new_dir)
            yield new_dir

    '''返回是否下载
    def whetherDownload(self,download):
        while True:
            yield download
    '''




    '''每日运行的抓图存图函数，第一次运行创建路径和数据文件，全量下/存图片，后续只下载ETag变动的图片并保存SHA1变动的图片，一轮完成而不是先head再get'''
    def runsDaily2(self):
        statistics_count = {'404':0,'Fail':0,'Ignore':0,'Added':0,'Update':0,'Replace':0}   #统计抓图状态
        update_history = {} #更新历史
        

        try:                        #读取图块更新史，……
            with open(self.data_folder+'/'+'update_history.json','r') as f:
                update_history = json.load(f)
        except FileNotFoundError:   #……若文件不存在（第一次爬）则创建它所在的目录
                if not os.path.exists(self.data_folder):
                    os.makedirs(self.data_folder)

        to_crawl = self.makePicXY(self.crawl_zones,self.target_depth)    #生成要抓取的图片坐标
        save_in  = self.getImgdir(self.image_folder)

        def visitPath(path):#抓取单张图片并对响应进行处理,图片存储在dir
            URL = self.map_domain + '/' + self.map_name + path + '.jpg?c=' + self.timestamp
            r=requests.head(URL,timeout=5)  #Head操作
            if r.status_code == 404 :       #【404，pass】
                print('404\t\t'+path)
            elif r.status_code == 200 :
                XY = self.path2xy(path,self.total_depth)
                file_name = reduce(lambda a,b:a+b ,map(str,[self.target_depth,'_',XY[0],'_',XY[1],'.jpg']))
                if file_name not in update_history :    #【库里无该图，Add】
                    response = self.downloadImage(URL)
                    update_history[file_name] = ([{'Save_in':next(save_in),'ETag':response['headers']['ETag']}])
                    with open(next(save_in)+file_name,'wb') as f:
                        f.write(response['image'])
                        f.close()
                    print('Adding\t'+path+'.jpg as '+file_name)
                else:
                    print(file_name+'\t\tin history,temporily ignore.')   
            return '1'

        with concurrent.futures.ThreadPoolExecutor (max_workers=self.max_threads) as executor:  #抓图工人池
            for index in executor.map(visitPath,to_crawl):
                pass
            
        with open(self.data_folder+'/'+'update_history.json','w') as f:#更新历史写回文件
            json.dump(update_history,f,indent=2)

        
            
#-----------------------------------------------------------------





    '''每天运行，探测选定区域内的图块信息但不下载图片，收录抓取日志，收录`活跃度`大型dict
    def runsDaily(self):
        Figure_404=Figure_Fail=Figure_ignore=Figure_added=Figure_update=Figure_changed=0
        
        download=False      #日更不下载图片，只收录图片头长度发生了变化的图块信息
        log_buffer={}
        download_queue=[]


        try:        #读取图块更新史，若文件不存在则其所在路径将被创建
            with open(self.data_folder+'/'+'update_history.json','r') as f:
                log_buffer=json.load(f)
                #log_buffer=ast.literal_eval(f.read())#txt to str to dict
        except FileNotFoundError:
            if not os.path.exists(self.data_folder):
                os.makedirs(self.data_folder)
            #with open(self.data_folder+'/'+'update_history.txt','w') as f:
            #    f.write('{}')#先写个空dict防呆

        to_crawl=self.makePicName(self.crawl_zones,self.target_depth)   #一个生成器
        save_to=self.getImgdir(self.image_folder,download)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:    #抓图工人线程池
            for pic_info in executor.map(self.dealWithPicurl,to_crawl,save_to,self.whetherDownload(download)): 
                if 'ERROR' in pic_info.keys():                                              #抓取图片头失败,pass
                    print(pic_info['ERROR']+'\t\t\t'+pic_info['Filename'])
                    if (pic_info['ERROR']== '404'):
                        Figure_404  += 1
                    else :
                        Figure_Fail += 1
                else:#图片头抓取成功
                    try:
                        latest_ETag=pic_info['ETag']
                        stored_ETag=log_buffer[pic_info['Filename']] [-1] ['ETag']
                        if latest_ETag==stored_ETag:                                        #dict中的图片并未过时（判断标准：ETag不变）,ignore
                            print('ignoring\t\t'+pic_info['Filename'])
                            Figure_ignore += 1
                        else:                                                               #Etag变化，图片可能更新但不保准，在此下载判断。
                            try:
                                img,filename = self.dealWithPicurl(pic_info['Path'],save_to,True)
                                latest_img = log_buffer[filename][-1]['Save_in']+filename
                                with open (latest_img,'rb') as fh:
                                    if hashlib.sha1(fh.read()).hexdigest()!=hashlib.sha1(img).hexdigest():   #图片内容更新了，存盘并为dict追加新的时间信息。
                                        if next(save_to) == log_buffer[filename][-1]['Save_in'] :
                                            print('Replaced\t\t'+pic_info['Filename'])         #同一天跑的两次之间图片发生了更新，替换同名的较旧文件，修改dict最后一条，Replace，
                                            del log_buffer[filename][-1]
                                        else:
                                            print('(Updated)\t\t'+pic_info['Filename']) #否则(两张图片虽然不同但不在一天)，ignore
                                            
                                        log_buffer[filename].append({'Save_in':next(save_to),'ETag':pic_info['ETag']})#并且向dict追加图片信息并保存图片
                                        with open(next(save_to)+filename,'wb') as f:
                                            f.write(img)
                                            f.close()
                                        Figure_changed += 1
                                    else:                       #图片内容未更新,ignore
                                        Figure_ignore += 1
                                        print('Updated\t\t'+pic_info['Filename'])
                            except requests.exceptions.ReadTimeout:
                                print('Failed'+'\t\t\t'+pic_info['Filename'])
                                Figure_Fail += 1
                            Figure_update += 1
                    except KeyError:                                                      #添加新图片信息,add
                        save_to=self.getImgdir(self.image_folder,True)
                        log_buffer[pic_info['Filename']]=([{'Save_in':next(save_to),'ETag':pic_info['ETag']}])
                        print('adding new img\t\t'+pic_info['Filename'])
                        #img,filename = self.dealWithPicurl(pic_info['Path'],save_to,True)
                        #with open(next(save_to)+filename,'wb') as f:
                        #    f.write(img)
                        #    f.close()
                        Figure_added += 1


        print('\n404:\t\t',Figure_404,'\nFailed:\t\t',Figure_Fail,'\nUnchanged:\t',Figure_ignore,\
        '\nAdded:\t\t',Figure_added,'\nUpdated:\t',Figure_update,'\nActually Changed:\t',Figure_changed)

        #with open(self.data_folder+'/'+'update_history.json','w') as f:#写回 图块更新史文件
        #    json.dump(log_buffer,f,indent=2)
        '''


'''每周运行，和上周的图片信息比较并下载Etag的图块，保存SHA1变动了的图片到当天的文件夹里，收录`图片历史`大型json。
'''

#cr.runsDaily()
'''
for i in cr.makePicXY([ ( (12,4),4,8 ) , ((-12,-4),4,8 ) ] , 0):
    pass
'''


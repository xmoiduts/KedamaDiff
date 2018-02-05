import requests,os
import time,datetime
import concurrent.futures
import ast,json
import hashlib
import logging
import itertools

class crawler(): #以后传配置文件
    def __init__ (self):
        '''文件/路径设置'''
        self.map_domain='https://map.nyaacat.com/kedama'    #Overviewer地图地址
        self.map_name='v2_daytime'                          #地图名称
        self.image_folder=r'images/'+self.map_name
        self.data_folder =r'data/'+self.map_name
        '''抓取设置'''
        self.max_threads=16                                 #线程数

        #self.total_depth=15
        #self.target_depth= -3                              #目标图块的缩放级别
        self.crawl_zones=['/1/2/2/2/2/2/2/2','/0/3/3/3/3/3/3/3','/0/3/3/3/3/3/3/2','/1/2/2/2/2/2/2/3','/2/1/1/1/1/1/1/0','/2/1/1/1/1/1/1/1','/3/0/0/0/0/0/0/0','/3/0/0/0/0/0/0/1']
        # '/1/2/2/2/2/2/2/2','/0/3/3/3/3/3/3/3','/0/3/3/3/3/3/3/2','/1/2/2/2/2/2/2/3','/2/1/1/1/1/1/1/0','/2/1/1/1/1/1/1/1','/3/0/0/0/0/0/0/0','/3/0/0/0/0/0/0/1'
        self.crawl_level=12
        # https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1.jpg?c=1510454854  

    '''输入图块链接，返回              错误处理应该改进吧     '''
    def teaseImage(self,URL):
        r=requests.head(URL,timeout=5) #只取响应头而不下载响应体
        return r.headers
        #print(r.headers['Last-Modified'],r.headers['Content-Length']) #404-"KeyError"error.

    '''下载给定URL的文件并返回。'''
    def downloadImage(self,URL):
        r=requests.get(URL,stream='True')
        if r.status_code==200:
            img=r.raw.read()
            return img

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

    '''给定[（中心点（X,Y坐标），目标缩放深度下横向图块数量，纵向图块数量），……]，目标缩放深度，
    返回一个生成器，按照每列中由上到下，各列从左向右的顺序产出（img path，"Y_X_目标缩放深度.jpg"）
    * 坐标系中X向右变大，Y向上变大'''
    def makePicXY(self,zoneLists,target_depth): #func ([ ( (12,4),4,8 ) , …… ] , -2 )
        for center,width,height in zoneLists: #对给定的**一个**区域生成坐标
            X_list= [X for X in range(center[0]-width*2**-target_depth ,center[0]+width*2**-target_depth ) if (X / (2**-target_depth)) %2 == 1]
            Y_list= [Y for Y in range(center[1]+height*2**-target_depth,center[1]-height*2**-target_depth,-1) if (Y / (2**-target_depth)) %2 == 1]
            #print(X_list,Y_list)
            for XY in itertools.product(X_list,Y_list): #求两个列表的笛卡尔积
                yield self.xy2Path(XY)

    def xy2Path(self,XY):
        print("Inbound:",XY)
    
    def path2xy(self,path):
        print("Inbound:",path)

    '''抓图线程'''
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

    '''永远返回/image_folder/年月日'''
    '''我也不想写这个的，但是直接传image_folder值,后面的executor.map()就只能执行17次'''
    def getImgdir(self,path,download):
        today=datetime.datetime.today().strftime('%Y%m%d')
        new_path=path+'/'+today+'/'
        if download==True:
            if not os.path.exists(new_path):
                os.makedirs(new_path)
        while(True):
            yield new_path

    '''返回是否下载'''
    def whetherDownload(self,download):
        while True:
            yield download

    '''每天运行，探测选定区域内的图块信息但不下载图片，收录抓取日志，收录`活跃度`大型dict'''
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

        to_crawl=self.makePicName(self.crawl_zones,self.crawl_level)   #一个生成器
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
        


'''每周运行，和上周的图片信息比较并下载Etag的图块，保存SHA1变动了的图片到当天的文件夹里，收录`图片历史`大型json。
'''
cr=crawler()
#cr.runsDaily()
for i in cr.makePicXY([ ( (12,4),4,8 ) , ((-12,-4),4,8 ) ] , 0):
    pass


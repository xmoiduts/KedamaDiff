import requests,os
import time,datetime
import concurrent.futures
import ast

# https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1/1.jpg?c=1510454854  
map_domain='https://map.nyaacat.com/kedama'
map_name='v2_daytime'
refcode='c=1510454854'
image_folder=r'images/'+map_name
data_folder =r'data/'+map_name
max_threads=32
crawl_zones=['/1/2/2/2','/0/3/3/3','/0/3/3/2','/1/2/2/3','/2/1/1/0','/2/1/1/1','/3/0/0/0','/3/0/0/1']
#'/1/2/2/2','/0/3/3/3','/0/3/3/2','/1/2/2/3','/2/1/1/0','/2/1/1/1','/3/0/0/0','/3/0/0/1'
crawl_level=8

'''输入图块链接，返回              错误处理应该改进吧     '''
def teaseImage(URL):
    r=requests.head(URL,timeout=5) #只取响应头而不下载响应体
    return r.headers
    #print(r.headers['Last-Modified'],r.headers['Content-Length']) #404-"KeyError"error.

'''下载给定URL的文件并重命名保存到指定目录中。'''
def downloadImage(URL,path,filename):
    r=requests.get(URL,stream='True')
    if r.status_code==200:
        img=r.raw.read()
        with open(path+filename,'wb') as f:
            f.write(img)
            f.close()

'''输入[多个可采集区域的前缀]和采集深度，生成['pic_name','文件名']。'''
def makePicName(prefixes,depth):
    for i in range(len(prefixes)):                                          #对于每个给定的采集区域
        prefix = prefixes[i].split('/')[1:]                                 #求出其自身的深度
        exp = depth - len(prefix)                                           #并计算元组深度
        for j in range(4**exp):                                             #Example: 4^(6-3)=64，Eg.:34
            to_split  = '0'*(2*exp-len(bin(j)[2:]))+bin(j)[2:]              #000000100010
            split_ed  = [to_split[k:k+2] for k in range(0,len(to_split),2)] #00 00 00 10 00 10
            quaternary= [str(int(i,2)) for i in split_ed]                   #0 0 0 2 0 2
            yield(['/'+'/'.join(prefix+quaternary)+'.jpg','_'+'_'.join(prefix+quaternary)+'.jpg'])
    return

'''抓图线程'''
def dealWithPicurl(pic_tuple,save_to,download=False):
    url=map_domain+'/'+map_name+pic_tuple[0]+'?'+refcode
    filename=pic_tuple[1]
    try:
        
        if download==True:
            downloadImage(url,save_to,filename)
            return {'Filename':filename}
        else:
            rh=teaseImage(url)
            return({'Path':pic_tuple,'Filename':filename,'Lastmod':rh['Last-Modified'],'ETag':rh['ETag']})
    except KeyError:
        return({'Filename':filename,'ERROR':'404'})
    except requests.exceptions.ConnectionError:
        return({'Filename':filename,'ERROR':'Fail'})
    except requests.exceptions.ReadTimeout:
        return({'Filename':filename,'ERROR':'Fail'})

'''永远返回/image_folder/年月日'''
'''我也不想写这个的，但是直接传image_folder值,后面的executor.map()就只能执行17次'''
def getImgdir(path,download):
    today=datetime.datetime.today().strftime('%Y%m%d')
    new_path=path+'/'+today+'/'
    if download==True:
        if not os.path.exists(new_path):
            os.makedirs(new_path)
    while(True):
        yield new_path

'''返回是否下载'''
def whetherDownload(download):
    while True:
        yield download

'''每天运行，探测选定区域内的图块信息但不下载图片，收录抓取日志，收录`活跃度`大型dict'''
def runsDaily():
    figure_404=figure_Fail=Figure_ignore=Figure_added=Figure_update=0
    download=False      #日更不下载图片，只收录图片头长度发生了变化的图块信息
    log_buffer={}
    download_queue=[]


    try:        #读取图块更新史，若文件不存在则空文件将被创建
        with open(data_folder+'/'+'update_history.txt','r') as f:
            log_buffer=ast.literal_eval(f.read())#txt to str to dict
    except FileNotFoundError:
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
        with open(data_folder+'/'+'update_history.txt','w') as f:
            f.write('{}')#先写个空dict防呆

    to_crawl=makePicName(crawl_zones,crawl_level)   #一个生成器
    save_to=getImgdir(image_folder,download)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:    #抓图工人线程池
        for pic_info in executor.map(dealWithPicurl,to_crawl,save_to,whetherDownload(download)): 
            if 'ERROR' in pic_info.keys():                                              #抓取图片头失败,pass
                print(pic_info['ERROR']+'\t\t\t'+pic_info['Filename'])
                if (pic_info['ERROR']== '404'):
                    figure_404  += 1
                else :
                    figure_Fail += 1
            else:
                try:
                    latest_ETag=pic_info['ETag']
                    stored_ETag=log_buffer[pic_info['Filename']] [-1] ['ETag']
                    if latest_ETag==stored_ETag:                                    #dict中的图片并未过时（判断标准：大小不变）,ignore
                        print('ignoring\t\t'+pic_info['Filename'])
                        Figure_ignore += 1
                    else:                                                               #为dict中的图片追加新的时间信息,update
                        log_buffer[pic_info['Filename']].append({'Save_in':next(save_to),'ETag':pic_info['ETag']})
                        print('Updated\t\t\t'+pic_info['Filename'])
                        download_queue.append(pic_info['Path'])
                        Figure_update += 1
                except KeyError:                                                        #添加新图片信息,add
                    log_buffer[pic_info['Filename']]=([{'Save_in':next(save_to),'ETag':pic_info['ETag']}])
                    print('adding new img\t\t'+pic_info['Filename'])
                    download_queue.append(pic_info['Path'])
                    Figure_added += 1

    print('\n404:\t\t',figure_404,'\nFailed:\t\t',figure_Fail,'\nUnchanged:\t',Figure_ignore,\
    '\nAdded:\t\t',Figure_added,'\nUpdated:\t',Figure_update)

    with open(data_folder+'/'+'update_history.txt','w') as f:#写回 图块更新史文件
        f.seek(0)
        f.write(str(log_buffer))
        f.truncate()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:    #增量？下载图片
        for pic_info2 in executor.map(dealWithPicurl,download_queue,getImgdir(image_folder,True),whetherDownload(True)): 
            if 'ERROR' in pic_info2.keys():                                              #抓取图片头失败,pass
                print(pic_info2['ERROR']+'\t\t\t'+pic_info2['Filename'])
            else:
                print('Downloaded'+'\t\t'+pic_info2['Filename'])

'''每周运行，和上周的图片信息比较并下载长度变化了的图块，保存图片到当天的文件夹里，收录`图片历史`大型dict。
'''
runsDaily()
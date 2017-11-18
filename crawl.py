import requests,os
import time,datetime
import concurrent.futures

# https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1/1.jpg?c=1510454854  
map_domain='https://map.nyaacat.com/kedama'
map_name='v2_daytime'
refcode='c=1510454854'
download_path=r'images/'+map_name
max_threads=32
crawl_zones=['/1/2/2/2/3/2']
#'/1/2/2/2','/0/3/3/3','/0/3/3/2','/1/2/2/3','/2/1/1/0','/2/1/1/1','/3/0/0/0','/3/0/0/1'
crawl_level=8

'''输入图块链接，返回              错误处理应该改进吧     '''
def teaseImage(imageURL):
    r=requests.head(imageURL,timeout=5) #只取响应头而不下载响应体
    return r.headers
    #print(r.headers['Last-Modified'],r.headers['Content-Length']) #404-"KeyError"error.

'''下载给定URL的文件并重命名保存到指定目录中。'''
def downloadImage(imageURL,path,filename):
    r=requests.get(imageURL,stream='True')
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
def dealWithPicurl(pic_tuple,save_dir,download=False):
    url=map_domain+'/'+map_name+pic_tuple[0]+'?'+refcode
    filename=pic_tuple[1]
    try:
        rh=teaseImage(url)
        if download==True:
            downloadImage(url,save_dir,filename)
        return({'Filename':filename,'Lastmod':rh['Last-Modified'],'Length':rh['Content-Length']})
    except KeyError:
        return({'Filename':filename,'ERROR':'404'})
    except requests.exceptions.ConnectionError:
        return({'Filename':filename,'ERROR':'Fail'})
    except requests.exceptions.ReadTimeout:
        return({'Filename':filename,'ERROR':'Fail'})

'''永远返回/download_path/年月日'''
'''我也不想写这个的，但是直接传download_path值,后面的executor.map()就只能执行17次'''
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
    download=False
    log_buffer={}
    to_crawl=makePicName(crawl_zones,crawl_level)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for pic_info in executor.map(dealWithPicurl,to_crawl,getImgdir(download_path,download),whetherDownload(download)): 
            if 'ERROR' in pic_info.keys():                                              #抓取图片头失败,pass
                print(pic_info['Filename'],pic_info['ERROR'])
            else:
                try:
                    latest_length=pic_info['Length']
                    stored_length=log_buffer[pic_info['Filename']] [-1] ['Length']
                    if latest_length==stored_length:                                    #dict中的图片并未过时,ignore
                        print('ignoring\t\t'+pic_info['Filename'])
                    else:                                                               #为dict中的图片更新时间,append
                        log_buffer[pic_info['Filename']].append([{'Lastmod':pic_info['Lastmod'],'Length':pic_info['Length']}])
                        print('Updated\t\t\t'+pic_info['Filename'])
                except KeyError:                                                        #添加新图片信息,assign
                    log_buffer[pic_info['Filename']]=([{'Lastmod':pic_info['Lastmod'],'Length':pic_info['Length']}])
                    print('adding new img\t\t'+pic_info['Filename'])
    print(log_buffer)

'''每周运行，和上周的图片信息比较并下载长度变化了的图块，保存图片到当天的文件夹里，收录`图片历史`大型dict。
'''
runsDaily()
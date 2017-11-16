import requests,os
import time,datetime
import concurrent.futures

# https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1/1.jpg?c=1510454854  
map_domain='https://map.nyaacat.com/kedama'
map_name='v2_daytime'
refcode='c=1510454854'
download_path=r'images/'
max_threads=32
crawl_zones=['/1/2/2/2','/0/3/3/3','/0/3/3/2','/1/2/2/3','/2/1/1/0','/2/1/1/1','/3/0/0/0','/3/0/0/1']
crawl_level=8

'''输入图块链接，返回              错误处理应该改进吧     '''
def teaseImage(imageURL):
    r=requests.head(imageURL) #只取响应头而不下载响应体
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

'''新建今日存图目录并以此替换download_path'''
def createTodayDir(path):
    today=datetime.datetime.today().strftime('%Y%m%d')
    new_path=path+'/'+today+'/'
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    return new_path #形如 20081223

'''抓图线程'''
def dealWithPicurl(pic_tuple,save_dir,download=False):
    url=map_domain+'/'+map_name+pic_tuple[0]+'?'+refcode
    filename=pic_tuple[1]
    try:
        rh=teaseImage(url)
        if download==True:
            downloadImage(url,save_dir,filename)
        return(filename+'\t'+rh['Last-Modified']+'\t'+rh['Content-Length'])
    except KeyError:
        return(filename+'\t'+'ERROR')

'''永远返回/download_path/年月日'''
'''我也不想写这个的，但是直接传download_path值,后面的executor.map()就只能执行17次'''
def getImgdir():
    download_dir=createTodayDir(download_path)
    while(True):
        yield download_dir

to_crawl=makePicName(crawl_zones,crawl_level)
with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    for pic_info in executor.map(dealWithPicurl,to_crawl,getImgdir()):   #没写是否下载的逻辑呢
        print(pic_info)

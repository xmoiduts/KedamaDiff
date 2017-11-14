import requests,os
import time,datetime

# https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1/1.jpg?c=1510454854  
map_domain='https://map.nyaacat.com/kedama'
map_name='v2_daytime'
refcode='c=1510454854'
download_path=r'images/'
threads=10

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
    return new_path


download_path = createTodayDir(download_path)
for i in makePicName(['/1/2/2/2','/0/3/3/3','/0/3/3/2','/1/2/2/3','/2/1/1/0','/2/1/1/1','/3/0/0/0','/3/0/0/1'],8):
    picurl=map_domain+'/'+map_name+i[0]+'?'+refcode
    try:
        rh=teaseImage(picurl)
        downloadImage(picurl,download_path,i[1])
        print(i[1],'\t',rh['Last-Modified'],'\t',rh['Content-Length'])
    except KeyboardInterrupt:
        break#以后改成退出程序
    except KeyError:
        print(i[1],'\t','ERROR')
    except:
        pass
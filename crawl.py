import requests,time
# https://map.nyaacat.com/kedama/v2_daytime/0/3/3/2/3/2/3/1/1.jpg?c=1510454854  
map_domain='https://map.nyaacat.com/kedama'
map_name='v2_daytime'
pic_name='/0/3/3/2/3/2/3/1/1.jpg'
refcode='c=1510454854'
picurl=map_domain + '/' + map_name + pic_name + '?' + refcode
#print(picurl)

def saveImage(imageURL):
    r=requests.head(imageURL) #只取响应头而不下载响应体
    print(r.headers['Last-Modified']) #404-"KeyError"error.

saveImage(picurl)

#输入[多个可采集区域的前缀]和采集深度，生成[pic_name 文件名]。
def makePicName(prefixes,depth):
    for i in range(len(prefixes)):                                          #对于每个给定的采集区域
        prefix = prefixes[i].split('/')[1:]                                 #求出其自身的深度
        exp = depth - len(prefix)                                           #并计算元组深度
        for j in range(4**exp):                                             #Example: 4^(6-3)=64，Eg.:34
            to_split = '0'*(2*exp-len(bin(j)[2:]))+bin(j)[2:]               #000000100010
            split_ed=[to_split[k:k+2] for k in range(0,len(to_split),2)]    #00 00 00 10 00 10
            quaternary=[str(int(i,2)) for i in split_ed]                         #0 0 0 2 0 2
            yield(['/'+'/'.join(prefix+quaternary)+'.jpg','_'+'_'.join(prefix+quaternary)+'.jpg'])
    return
        

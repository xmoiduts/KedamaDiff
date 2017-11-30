import requests,os
import ast,json
import hashlib

map_name = 'v2_daytime'
data_folder = r'data/'+map_name

with open(data_folder+'/'+'update_history.json','r') as f:           #读取字符串并‘执行’
    log_buffer = json.load(f)#txt to str to dict
counter = 0
for key in log_buffer:
    
    try:
        a = 0#当前已经确保不重复的图块
        while True:
            filename_a = log_buffer[key][-a-1]['Save_in']+key
            filename_b = log_buffer[key][-a-2]['Save_in']+key
            try:
                with open (filename_a,'rb') as fh_a, open(filename_b,'rb') as fh_b:
                    if(hashlib.sha1(fh_a.read()).hexdigest() != hashlib.sha1(fh_b.read()).hexdigest()):   #hash不同
                        #print(filename_a,'Different With Earlier img')
                        a += 1
                    else:                                                                               #hash相同
                        print('Delete value',filename_a)
                        counter += 1
                        del log_buffer[key][-a-1]#还要真删除文件
            except FileNotFoundError as not_found:#这段贼脏
                if not_found.filename == filename_a:
                    print('1 not found')
                    del log_buffer[key][-a-1]
                else:
                    del log_buffer[key][-a-2]
                    print('2 not found')
                
    except IndexError:
        pass
#print(log_buffer)
print(counter)
with open(data_folder+'/'+'update_history.json','w') as f:#写回 文件
    json.dump(log_buffer,f,indent=2)
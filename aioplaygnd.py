import asyncio
import aiohttp
import cv2
import numpy as np
from crawl import crawler
from configs.crawl_list import CrawlList as map_list
import time

def get_opencv_img_from_buffer(buffer, flags):
    bytes_as_np_array = np.frombuffer(buffer, dtype=np.uint8)
    return cv2.imdecode(bytes_as_np_array, flags)

async def downloadImage(sess, sema, URL):
    async with sema:
        #if '/0/3/3/1/1/1/0/3.jpg' in URL:
        #    time.sleep(10)
        async with sess.head(URL) as response:
            cc = response.headers['ETag'] # ETag here
            print (cc)
            '''
            if response.status == 200:
                img = await response.read()
                img2 = get_opencv_img_from_buffer(img, cv2.IMREAD_ANYCOLOR)
                cv2.imshow('aa',img2)
                cv2.waitKey(1)
            print(URL, response.status)
            return str(response.status)
            '''

            
    # return image and headers

async def jobs():
    semaphore = asyncio.Semaphore(4)
    for map_name, map_conf in map_list.items():
        if map_conf.enable_crawl == True:
            cr = crawler(map_conf, noFetch = True)
            to_crawl = cr.makePath(cr.crawl_zones, cr.target_depth)
            URLS = ['{}/{}{}.jpg?c={}'.format(cr.map_domain, cr.map_name, path, cr.timestamp) for path in to_crawl]
            async with aiohttp.ClientSession(read_bufsize = 2**20) as sess:
                for i in await asyncio.gather(*[downloadImage(sess, semaphore, url) for url in URLS]):
                    print(i)



def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(jobs())

if __name__ == '__main__':
    main()

# concurrent.futures.ThreadPoolExecutor 里面的queue 有一处用到了blocking=True的Flag，
# 这是否是他进行worker控制的原理呢？还是_adjust_thread_count 起到了作用？
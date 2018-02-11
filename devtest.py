import sys
import crawl

try:
    test_number=int(sys.argv[1])
    print('Running test', test_number)
except ValueError:
    print("A int number required")
    exit(0)

'''以下是测试项'''

if test_number == 101:
    cr=crawl.crawler(test=True)
    cr.fetchTotalDepth()

elif test_number == 102:
    cr=cr=crawl.crawler(test=True)
    cr.xy2Path((-100,96),15)
    
elif test_number == 103:
    cr=crawl.crawler()
    toCrawl = cr.makePicXY([((12,4),4,8)],-3)  
    for i in toCrawl:
        print(i)




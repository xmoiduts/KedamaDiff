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
    cr=crawl.crawler(test=True)
    cr.xy2Path((-24,8),15)
    
elif test_number == 103:
    cr=crawl.crawler()
    toCrawl = cr.makePicXY([((12,4),4,8)],-3)  
    for i in toCrawl:
        print(i)

elif test_number == 104:
    cr=crawl.crawler(test=True)    
    cr.xy2Path((cr.path2xy('/0/0',2)),3)

elif test_number == 105:
    cr=crawl.crawler(test=False)
    gen1=cr.makePicXY([((0,0),4,2)],cr.target_depth)
    for XY in gen1:
        print(XY)
    #/0        /3/3/2/3/2/3/1
    #/0/3/3/3/3/3/3/2/3/2/3/1

elif test_number == 106:
    cr=crawl.crawler(test=True)
    print(cr.path2xy('/0/3/3/3/3/3/3/2/3/2/3/1',15))

elif test_number == 107:
    cr=crawl.crawler()
    cr.changeImgName()

elif test_number == 108:
    cr=crawl.crawler()
    cr.changeJsonKey()

elif test_number == 109:
    cr=crawl.crawler(test=True)
    cr.runsDaily2()
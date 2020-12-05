import sys
import crawl
import joiner
import joiner2

def main():

    try:
        test_number=int(sys.argv[1])
        print('Running test', test_number)
    except ValueError:
        print("A int number required")
        exit(0)

    '''以下是测试项'''

    '''
    if test_number == 101:
        cr=crawl.crawler(test=True)
        cr.fetchTotalDepth()

    elif test_number == 102:
        cr=crawl.crawler(test=True)
        cr.xy2Path((-24,8),15)
        
    elif test_number == 103:
        cr=crawl.crawler()
        toCrawl = cr.makePath([((12,4),4,8)],-3)  
        for i in toCrawl:
            print(i)

    elif test_number == 104:
        cr=crawl.crawler(test=True)    
        cr.xy2Path((cr.path2xy('/0/0',2)),3)

    elif test_number == 105:
        cr=crawl.crawler(test=False)
        gen1=cr.makePath([((0,0),2,2)],cr.target_depth)
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
        cr.runsDaily()

    elif test_number == 110:
        cr=crawl.crawler(test=True)
        for x in list(cr.makePath(cr.crawl_zones, cr.target_depth)):
            print(cr.path2xy(x,cr.total_depth))

    '''

    if test_number == 111:
        jr=joiner.joiner()
        print(list(jr.makeImgName([((12,4),4,8)],-3)))
    elif test_number == 112:
        jr = joiner.joiner()
        jr.doAJob(jr.makePic1,[((0, -8), 56, 29)],-3,'20180903')
    elif test_number == 113:
        jr = joiner.joiner()
        jr.doAJob(jr.makePic2,[((0, -8), 56, 29)],-3,'20180625','20180929')
    elif test_number == 114:
        jr = joiner.joiner()
        jr.matrixJob(jr.makeMatrix,((0, -8), 56, 29),-3,'20180710','20180813')
    elif test_number == 115:
        jr2 = joiner2.joiner(mode = 'show')
        jr2.joinerjob('v2_daytime',((0, -8), 56, 29),'20181010')
    elif test_number == 116:
        jr2 = joiner2.joiner(mode = 'show')
        for i in range(8, 29):
            datestr = '202011{:02d}'.format(i)
            jr2.joinerjob('v4',((32, -16), 4, 2), datestr)

            # 鸟笼： ((88,40),5,4)

if __name__ == '__main__':
    main()
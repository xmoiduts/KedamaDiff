# KedamaDiff, <ruby><rb>毛玉</rb><rt>Kedama</rt><rb>差分</rb><rt>Diff</rt></ruby>
Crawl Minecraft Overviewer-based maps and save updated map tiles, in hope to view them later [to do]
抓取 Overviewer 渲染的 Minecraft 地图并差分入库，以期日后可以查看Overviewer历史地图[待做]

WARNING: project in early development, unstable, and no backward compatibility by design. If you happen to have used it and generated some json files, try to comprehend tools/json-to-sqlite that I use to convert data file.
警告，本项目尚处于早期开发阶段，无力保证向下兼容（兼容以前的数据文件/配置文件格式）。 tools/json-to-sqlite 目录提供了作者自用的数据转换脚本。

[Dev log Ep.1 <Zh-CN> | 开发日志其一](https://xmoiduts.github.io/2021/07/29/KedamaDiff%E5%BC%80%E5%8F%91%E6%97%A5%E5%BF%97-%E5%85%B6%E4%B8%80/)  

[Dev log Ep.2 <Zh-CN> | 开发日志其二](https://xmoiduts.github.io/2021/08/05/KedamaDiff%E5%BC%80%E5%8F%91%E6%97%A5%E5%BF%97-%E5%85%B6%E4%BA%8C/)

# Requirements 依赖

Python 3.7+

pip3

​	pytz \<unify timezone|统一时区\>

​	python-telegram-bot \<Send status message to Telegram | 向TG发送运行状态/进度消息\>

​	aiohttp \<single-threaded concurrency|单线程并发抓取\>

​	boto3 \<S3 support|S3 对象存储支持库\>



# Usage 用法

Modify configs/config.py (general settings) and configs/crawl_list.py (map settings), instructions are in those files.

修改上述文件（抓取器设置，地图设置），备注见文件内注释。

`cd` to directory that have `crawl.py` ，run `python3 ./crawl.py`. Look at its console log output, use `ctrl+c` to terminate when error happens.

`cd` 到 `crawl.py` 所在目录，运行 `python3 ./crawl.py`，观察终端输出，有异常时`ctrl+c`关停程序

## 将抓取所获上传到 (Upload to) S3

在配置文件中配置`S3Config()`相关信息，将config.py的storage_type改为`S3`

运行 `python3 syncer.py [1] [2]/data DB --having sqlite3` 上传抓取记录数据库，已上传的数据库文件不会从本地删除。

运行 `python3 syncer.py [1] [2]/images img` 上传已抓取图片，**已成功上传的图片会从本地文件系统删除**

(Optional, unmature | 可选不成熟功能) 运行 `python3 syncer.py [1] [2]/log log --having .log ` 上传运行日志，已上传的日志**不**会从本地删除。

其中 [1] 为 config.py/CrawlerConfig.project_root, 默认值 `..` ; [2] 为  config.py/CrawlerConfig.data_foldersW, 默认值 `data-production`

# Script behavior 脚本会做什么

Under default configurations, crawl.py will create data folder at `..` , assume crawl.py in `.` ;

data folder have 3 sub-folders:

- data (crawl records)
- images (saved images)
- log (log)


# recursively upload files in image folders to S3(-compatible services)
# (optional: ) and delete source files upon upload success.
# TODO: upload (compressed) data to S3.

import boto3
import os
from configs.config import S3Config as S3Conf
from concurrent.futures import ThreadPoolExecutor
import argparse
from itertools import repeat

session = boto3.session.Session()
client = session.client(
    's3',
    region_name = S3Conf.region_name,
    endpoint_url = S3Conf.endpoint_url,
    aws_access_key_id = S3Conf.RW_key,
    aws_secret_access_key = S3Conf.RW_secret
)

extra_args_img = { 
    'ContentType': 'image/jpeg',
    'ContentDisposition': 'inline',
    'ACL': 'public-read'
}
extra_args_db = { 
    'ContentType': 'application/octet-stream',
    'ContentDisposition': 'attachment',
    'ACL': 'public-read'
}
extra_args_log = { 
    'ContentType': 'text/plain',
    'ContentDisposition': 'attachment',
    'ACL': 'public-read'
}

# recursively upload files
# 这脚本上传刚写入的图片，故使用写目录。
# TODO: optimize path-joining readability
# Q: why not using os.path.join? A: delimiter between *nix and windows are different, causing mix.
def generateFilePath(dir, having):
    for root, _, file_names in os.walk(dir):
        # root: 'proj_root\\path1\\path2\\...', delimeter in ['\\', '/'], we normalize to '/'.
        root_norm = root.replace('\\','/') # 'proj_root/root/path1/path2/...'
        sub_root_norm = '/'.join(root_norm.split('/')[1:]) # 'path1/path2/...'
        for file_name in file_names:
            local = '/'.join([root_norm, file_name])
            remote = '/'.join([sub_root_norm, file_name])
            if having in local: yield local, remote
            else: continue
            #print(local, ' | ', remote)
            #break

# 上传文件，在上传成功后如文件是图片则将local加入待删列表，以供后续删除
# 在以下位置检查文件类型的正确性：
#   上传文件到S3前 检查local file path的后缀名是否和mode匹配
#   将local filepath加入待删除列表前 检查mode是否是img
#   在删除文件前检查file path后缀名是否为jpg
def uploadFileToS3(L_R, mode):
    local, remote = L_R[0], L_R[1] # TODO: 在输入层解压此元组
    mode_table = {'img': extra_args_img, 'DB': extra_args_db, 'log': extra_args_log}
    mode_suffix= {'img': '.jpg', 'DB': '.sqlite3', 'log': '.log'}
    assert mode in mode_table
    assert local.endswith(mode_suffix[mode])
    try:
        client.upload_file( 
            local,
            'kedamadiff-project',
            remote,
            ExtraArgs = mode_table[mode]
        )
        print(local, '\tuploaded')
        return (True, local)
    except KeyboardInterrupt as e:
        raise e
    except Exception as e:
        print('exception: ', str(e))
        return (False, '')
    
    # TODO: 503 slow down: 指数回退



 
def sync(root, dir_from_root, mode = 'img', having=''):
    # sync all files in dir having 'having' substring in their paths.
    # root: project root, e.g.: '..'
    # WARNING NOTE: 使用concurrent做并发非我本意，实为还没用过async boto3;以后你要每秒并发加载上百张图片，concurrent.futures或许会太浪费/低效了吧
    with ThreadPoolExecutor(max_workers=15) as exec:
        try:
            for msg in exec.map(
                uploadFileToS3, 
                generateFilePath(
                    '/'.join([root, dir_from_root]), 
                    having=having),
                repeat(mode)
                ):
                if mode == 'img' and msg[0]: del_list.append(msg[1]) # 只删图片不删其他
                else: continue
        except KeyboardInterrupt:
            print('ctrl+c, exit.')
            raise KeyboardInterrupt

def delete_images(file_list):
    for file_path in file_list:
        if file_path.endswith('.jpg'): # only delete images 
            os.remove(file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("root", help="project root, e.g. \'..\'")
    parser.add_argument("dir_from_root")
    parser.add_argument("mode", help = "file type, in img | DB | log ")
    parser.add_argument("--having", help = "files having \'having\' in their paths will be uploaded.")
    args = parser.parse_args()
    print(args.root, args.dir_from_root)
    print(args.having)
    global del_list
    del_list = []
    if args.having : sync(args.root, args.dir_from_root, mode = args.mode, having = args.having)
    else: sync(args.root, args.dir_from_root, mode = args.mode)
    delete_images(del_list)
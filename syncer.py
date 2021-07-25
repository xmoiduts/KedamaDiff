# recursively upload files in image folders to S3-compatible services
# (optional: ) and delete source files upon upload success.

import boto3
import os
from configs.config import S3Config as S3Conf
from concurrent.futures import ThreadPoolExecutor
import argparse

session = boto3.session.Session()
client = session.client(
    's3',
    region_name = S3Conf.region_name,
    endpoint_url = S3Conf.endpoint_url,
    aws_access_key_id = S3Conf.RW_key,
    aws_secret_access_key = S3Conf.RW_secret
)

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

def uploadImageToS3(L_R):
    local, remote = L_R[0], L_R[1]
    client.upload_file( 
        local,
        'kedamadiff-project',
        remote,
        ExtraArgs={ 
            'ContentType': 'image/jpeg',
            'ContentDisposition': 'inline',
            'ACL': 'public-read'
        }
    )
    print(local, '\tuploaded')
    # TODO: 503 slow down: 指数回退
    # TODO: 上传成功后将local加入待删列表，以供后续删除

 
def sync(root, dir_from_root, having=''):
    # sync all files in dir having 'having' substring in their paths.
    # root: project root, e.g.: '..'
    # WARNING NOTE: 使用concurrent做并发非我本意，实为还没用过async boto3;以后你要每秒并发加载上百张图片，concurrent.futures或许会太浪费/低效了吧
    with ThreadPoolExecutor(max_workers=200) as exec:
        try:
            for msg in exec.map(
                uploadImageToS3, 
                generateFilePath(
                    '/'.join([root, dir_from_root]), 
                    having=having)
                ):
                pass
        except KeyboardInterrupt:
            print('ctrl+c, exit.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("root", help="project root, e.g. \'..\'")
    parser.add_argument("dir_from_root")
    parser.add_argument("--having", help = "files having \'having\' in their paths will be uploaded.")
    args = parser.parse_args()
    print(args.root, args.dir_from_root)
    print(args.having)
    if args.having : sync(args.root, args.dir_from_root, having = args.having)
    else: sync(args.root, args.dir_from_root)

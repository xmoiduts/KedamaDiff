# read and save images from / to local disk or s3.
# reserve 1 DB connection to it.
# TODO: Don't include source-website downloading here.
# TODO: proxy me


# import sqlite3
import logging
import os

from botocore.exceptions import ClientError





class ImageManager():
    # local 必选，S3 可选
    # todo: 读写路径不一致怎么办
    def __init__(self, logger, storage_type, project_root, data_foldersR, map_savename):
        if logger: 
            self.logger = logger
        else: 
            self.logger = logging.getLogger()
            self.logger.warning('logger not specified, using backup default logger.')
        self.proj_root = project_root
        assert storage_type in ['local', 'S3']
        self.preferred_storage_type = storage_type
        self.image_read_path = '{}/images/{}'.format(data_foldersR, map_savename) # 'data-dev/images/v2_daytime', 此path缺root
        self.default_write_path = None
        self.S3_enabled = False
        self.tExecutor = None # placeholder that avoids variable-inexist-exception.

    def addS3Info(self, S3_config):
        #raise NotImplementedError
        
        import boto3
        from botocore.exceptions import ClientError
        
        self.S3_session = boto3.session.Session()
        self.S3_client  = self.S3_session.client(
            's3',
            region_name = S3_config.region_name,
            endpoint_url = S3_config.endpoint_url,
            aws_access_key_id = S3_config.RW_key,
            aws_secret_access_key = S3_config.RW_secret
        )
        self.S3_enabled = True


    def setDefaultWritePath(self, path):
        # set self.default_path, which is the default image saving location\
        # ...if path is not explicitedly given in saveImage() method.
        # a typical path: 'data-dev/images/v2_daytime/20180202'
        self.default_write_path = path # 此path缺root BUG: read/write path不一致怎么办
        self.logger.info('img write path set to [{}]'.format(self.default_write_path))
    
    def saveImage(self, path, file_name, image):
        # save to local file system location:
        # [{project_root}/{data_foldersW}/images/{map_savename}/{date}]/{file_name}
        # why not save to S3? 'cause network interruption. Save locally and upload afterwards.
        # test if target directory exists, create if inexist.
        # TODO: what if disk is full?

        write_path = path or self.default_write_path
        assert write_path
        write_path = self.proj_root + '/' + write_path

        if not os.path.exists(write_path):
            os.makedirs(write_path)
            self.logger.info('Made directory\t./{}'.format(write_path))
        with open(write_path + '/' + file_name, 'wb') as f:
            f.write(image)
            f.close()

    def retrieveImage(self, date:str, file_name:str):
        # retrieve saved image by date and file_name
        # firstly from configured storage type, 
        # if not found then go to fallback storage type.
        # TODO: consider extracting one file from solidified file archives.

        if self.preferred_storage_type == 'local':
            image_getters = [self.getImageLocal, self.getImageS3]
        elif self.preferred_storage_type == 'S3':
            image_getters = [self.getImageS3, self.getImageLocal]
        for getter in image_getters: # attempt to get image from various ways.
            try:
                img = getter(date, file_name)
                #self.logger.debug("File {}/{} by {} is img with length {}.".format(date, file_name, getter, len(img)))
                if img: return img
            except FileNotFoundError: 
                self.logger.warning("File {}/{} not found in {}.".format(
                    date, file_name, getter.__name__)) 
                continue
        raise FileNotFoundError

    async def aRetrieveImage(self, date:str, file_name:str):
        # asynchronize the blocking method `retrieveImage`.

        import asyncio
        #import aiohttp
        from concurrent.futures import ThreadPoolExecutor
        if not self.tExecutor:
            self.tExecutor = ThreadPoolExecutor(max_workers = 20)
        loop = asyncio.get_running_loop()
        img = await asyncio.gather(loop.run_in_executor(self.tExecutor, self.retrieveImage, date, file_name))
        return img[0]

    def cleanUp(self):
        if self.tExecutor:
            self.tExecutor.shutdown()

    def getImageS3(self, date:str, file_name:str):
        #raise FileNotFoundError
        assert self.S3_enabled 
        remote = '{}/{}/{}'.format(self.image_read_path, date, file_name)
        #self.logger.debug('S3 getter accessing {}'.format(remote))
        from io import BytesIO
        local_buffer = BytesIO()
        try:
            self.S3_client.download_fileobj('kedamadiff-project', remote, local_buffer)
            local_buffer.seek(0)
            return local_buffer.read()
        except ClientError as e:
            if e.response['Error']['Code'] == '404': raise FileNotFoundError
            else: raise e

    def getImageLocal(self, date:str, file_name:str):
        with open('{}/{}/{}/{}'.format(self.proj_root, self.image_read_path, date, file_name), 'rb') as f:
            img = f.read()
        return img
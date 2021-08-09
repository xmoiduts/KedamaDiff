class CrawlerConfig():    
    storage_type = 'local' # read images from: 'local' or 'S3', new images always go to local, then upload to s3 separately.
    project_root = '..' # root contains [data-prod, data-dev, code-prod, code-dev,...]
    data_foldersW = '../data-dev' # where to write generated data to
    timezone = 'Asia/Shanghai' # Fix timezone since crawler is meant to be run on any location globally.
    enableTelegramNotify = True
    telegram_bot_key = '512345674:Adisfghvliszfnlisrugvliureliugc'
    telegram_msg_recipient = 1234567893
    crawl_request_timeout = 5 # in seconds
    crawl_request_retries = 5
class S3Config():
    bucket_name = 'kedamadiff-iaeulivniurg' 
    RW_key = 'O5CABCDEFGHIJKLMNOPQEL3' # An S3 API key that have RW write to bucket above
    RW_secret = 'Mdf-p84u5b9ovmtuo9e8tfcnou8n4btz0fc' # corresponding secret/credential
    region_name = 'NYC1'
    endpoint_url = 'https://nyc1.example.com'
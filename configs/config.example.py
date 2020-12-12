class CrawlerConfig():
    data_folders = '../data-dev' # where to store generated data
    timezone = 'Asia/Shanghai' # Fix timezone since crawler is meant to be run on any location globally.
    enableTelegramNotify = True
    telegram_bot_key = '512345674:Adisfghvliszfnlisrugvliureliugc'
    telegram_msg_recipient = 1234567893
    crawl_request_timeout = 5 # in seconds
    crawl_request_retries = 5

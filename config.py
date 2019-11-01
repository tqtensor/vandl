from pathlib import Path


home = str(Path.home())
db_user = 'vandl_dev'
db_pwd = 'cryptocean'
use_virtual_screen = True
conn_string = f'host=127.0.0.1 dbname = vietnam_stock user = {db_user} password = {db_pwd}'
download_path = './data/download'
historial_price = './data/initial_load/historical_price'
chrome_download_path = f'{home}/Downloads'

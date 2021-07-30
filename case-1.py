import os
from selenium import webdriver
from linenotipy import Line
from func.grab_stock import parameters,html_to_data,covert_to_excel
from func.update_googledrive import up_to_drive

class switch_location:
    def secrets():
        os.chdir('/Users/lailai/poetry-test')
        # os.chdir('/home/g401657316/python/poetry')
    def files():
        path = '/Users/lailai/poetry-test/data'
        # path = '/home/g401657316/python/poetry/data'
        if not os.path.isdir(path):
            os.makedirs(path)
        os.chdir(path)
    def drive():
        path = '/Users/lailai/chromedriver'
        # path = '/home/g401657316/python/chromedriver'
        return path

if __name__=='__main__':
    try:
        db_name,year,date = parameters()
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('user-agent="MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-browser-side-navigation')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        driver = webdriver.Chrome(executable_path=switch_location.drive(),options=options)
        html = html_to_data(driver,db_name,year,date)
        if html.run_or_not():
            pass
        else:
            print('Start to get data!')
            table1, table2, table3, table4 = html.all_html()
            print('Covert data to files!')
            switch_location.files()
            covert_to_excel.all_convert(covert_to_excel,table1, table2, table3, table4)
            switch_location.secrets()
            service = up_to_drive.get_secrets()
            print('Ready to update data!')
            switch_location.files()
            folder_link = up_to_drive.update_files(up_to_drive,service)
            line = Line(token='bCuyFu5YOXpNCnNzE5uEMpmGQSoN40A6u4efTeFMWlh')
            line.post(message=folder_link)
    except Exception as err:
        print(err)
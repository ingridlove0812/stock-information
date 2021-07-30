# -*- coding: UTF-8 -*-
import urllib.request
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0
from selenium.webdriver.support.ui import Select
import pandas as pd
from func.db_connect import connect_sql
import datetime
#from linenotipy import Line
from gspread_pandas import Spread
import re
import sys
import os
# os.chdir('/home/g401657316/python/poetry/')

def parameters():
    dbname = 'goodinfo'
    year = datetime.date.today().year
    date = str(datetime.date.today())
    return dbname,year,date

#從DB拉資料
def pull_data(db_name, select, string = 'null', integer = 'null'):
    conn, cur = connect_sql(db_name)
    cur.execute(select)
    cols = [i[0] for i in cur.description]
    tmp_data = cur.fetchall()
    tmp_data = pd.DataFrame(tmp_data, columns = cols)
    if string != 'null':
        tmp_data[string] = tmp_data[string].select_dtypes([np.object]).stack().str.decode('utf-8').unstack()
    if integer != 'null':
        for l in integer:
            tmp_data[l] = tmp_data[l].fillna(0).replace('',0).astype(int)
    cur.close()
    conn.close()
    return tmp_data

#刪除舊有的資料
def delete_data(db_name, table_name, table):
    delete = "DELETE FROM `%s`.`%s` WHERE date = '%s'" % (db_name, table_name, max(table['date']))
    conn, cur = connect_sql(db_name)
    cur.execute(delete)
    conn.commit()
    cur.close()
    conn.close()

#將資料塞回DB
def insert_data(db_name, table_name, table, tuple_data):#db_name = 'TVBS_Test'; table_name = 'GA_FireBase_Source_New_Three'; table = output
    conn, cur = connect_sql(db_name)
    cols = ("`,`".join([str(col) for col in table.columns.tolist()]))#.replace('articles','article').replace('click_num','fb_link_clicks')
    insert = "REPLACE INTO " + db_name + '.' + table_name + " (`" +cols + "`) VALUES (" + "%s,"*(len(tuple(table))-1) + "%s)"
    cur.executemany(insert, tuple_data)
    conn.commit()
    cur.close()
    conn.close()

class html_to_data():
    def __init__(self,driver,db_name,year,date):
        self.driver = driver
        self.db_name = db_name
        self.year = year
        self.date = date

    def run_or_not(self):
        lastest_date = pull_data(self.db_name, "SELECT MAX(date) FROM `transaction`")
        date_list = pull_data(self.db_name, "SELECT date FROM `day_off`")
        if str(lastest_date.iloc[0,0]) == self.date:
            print("Already lastest data!")
            return True
        elif self.date in date_list:
            print("Today is day-off!")
            return True
        else:
            success = False
            while not success:
                try:
                    print('test0')
                    url = "https://goodinfo.tw/StockInfo/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D+%28%E9%AB%98%E2%86%92%E4%BD%8E%29%40%40%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D%40%40%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E"
                    self.driver.get(url)
                    print('sleep')
                    time.sleep(30)
                    print(1)
                    print(self.year)
                    print(self.driver.find_element_by_xpath('//*[@id="row0"]/td[5]/nobr').text)
                    new_date = str(self.year) + '-' + self.driver.find_element_by_xpath('//*[@id="row0"]/td[5]/nobr').text.replace('/', '-')
                    print(2)
                    success = True
                except:
                    pass
                return str(lastest_date.iloc[0, 0]) == new_date

    def html1(self):
        success = False
        while not success:
            try:
                # print(0)
                # url = "https://goodinfo.tw/StockInfo/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D+%28%E9%AB%98%E2%86%92%E4%BD%8E%29%40%40%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D%40%40%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E"
                # self.driver.get(url)
                print(1)
                col = ['rank', 'code', 'name', 'market', 'date', 'deal', 'mixed_count', 'mixed_rate', 'deal_count',
                       'deal_amt', 'last_deal', 'opening', 'highest', 'lowest', 'bumpy_rate', 'per', 'pbr']
                # for d in range(1, 15):
                #     print(d)
                #element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '//*[@id="selRPT_TIME"]')))
                #dropdown = Select(element)
                #dropdown.select_by_index(1)
                time.sleep(10)
                table = self.driver.find_element_by_id('tblStockList').find_elements_by_xpath(".//tr")
                print(2)
                lst = []
                for i in table[:22]:
                    lst.append(i.text.replace("\n", " ").split(' ') if re.search('^[0-9]', i.text) else '')
                df = pd.DataFrame([l for l in lst if l != ''], columns=col)
                print(set(df.date))
                df.date.loc[:, ] = str(self.year) + '-' + df.date.str.replace('/', '-')
                df.deal_count.loc[:, ] = df.deal_count.str.replace(',', '')
                df.deal_amt.loc[:, ] = df.deal_amt.str.replace(',', '')
                delete_data(self.db_name, 'transaction', df)
                print(3)
                tuple_data = []
                for t, row in df.iterrows():
                    tuple_data.append(tuple(row))
                insert_data(self.db_name, 'transaction', df, tuple_data)
                print(4)
                select = "SELECT `date`,`rank`,`code`,`name`,`deal`,`mixed_count`,`mixed_rate`,`deal_amt` FROM `transaction` ORDER BY `date` DESC,`rank` LIMIT 200"
                result = pull_data(self.db_name, select)
                result.columns = ['日期','排名','代號','名稱','成交','漲跌價','漲跌幅','成交額(百萬)']
                success = True
                return result
            except:
                pass

    def html2(self):
        success = False
        while not success:
            try:
                # url = "https://goodinfo.tw/StockInfo/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D+%28%E9%AB%98%E2%86%92%E4%BD%8E%29%40%40%E6%88%90%E4%BA%A4%E9%87%91%E9%A1%8D%40%40%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E"
                # driver.get(url)
                print(1)
                col = ['rank', 'code', 'name', 'market', 'date', 'deal', 'mixed_count', 'mixed_rate', 'deal_count',
                       'deal_amt', 'last_deal', 'opening', 'highest', 'lowest', 'bumpy_rate', 'per', 'pbr']
                element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="selSHEET2"]')))
                dropdown = Select(element)
                dropdown.select_by_index(1)
                time.sleep(10)
                print(2)
                #element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="selRPT_TIME"]')))
                #dropdown = Select(element)
                #dropdown.select_by_index(1)
                #time.sleep(10)
                table = self.driver.find_element_by_id('tblStockList').find_elements_by_xpath(".//tr")
                print(3)
                lst = []
                for i in table[:22]:
                    lst.append(i.text.replace("\n", " ").split(' ') if re.search('^[0-9]', i.text) else '')
                df = pd.DataFrame([l for l in lst if l != ''], columns=col)
                print(set(df.date))
                df.date.loc[:, ] = (datetime.date.today() - datetime.timedelta(days = datetime.date.today().weekday()))
                print(set(df.date.astype(str)))
                df.deal_count.loc[:, ] = df.deal_count.str.replace(',', '')
                df.deal_amt.loc[:, ] = df.deal_amt.str.replace(',', '')
                delete_data(self.db_name, 'transaction_weekly', df)
                print(4)
                tuple_data = []
                for t, row in df.iterrows():
                    tuple_data.append(tuple(row))
                insert_data(self.db_name, 'transaction_weekly', df, tuple_data)
                print(5)
                select = "SELECT `date`,`rank`,`code`,`name`,`deal`,`mixed_count`,`mixed_rate`,`deal_amt` FROM `transaction_weekly` WHERE `date` >= DATE_ADD(CURDATE(),INTERVAL -WEEKDAY(CURDATE())-7 DAY)"
                result = pull_data(self.db_name, select)
                result.columns = ['日期','排名','代號','名稱','成交','漲跌價','漲跌幅','成交額(百萬)']
                success = True
                return result
            except:
                pass

    def html3(self):
        success = False
        while not success:
            try:
                url = "https://goodinfo.tw/StockInfo/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E7%B4%AF%E8%A8%88%E4%B8%8A%E6%BC%B2%E5%83%B9%E6%A0%BC+%28%E5%8D%8A%E5%B9%B4%29%40%40%E7%B4%AF%E8%A8%88%E4%B8%8A%E6%BC%B2%E5%83%B9%E6%A0%BC%40%40%E5%8D%8A%E5%B9%B4"
                self.driver.get(url)
                print(1)
                col = ['rank', 'code', 'name', 'deal', 'rise_count', 'rise_rate', 'deal_count', 'date', 'rise_count_curr_year',
                       'rise_rate_curr_year', 'rise_count_three_months', 'rise_rate_three_months', 'rise_count_half_year',
                       'rise_rate_half_year', 'rise_count_one_year', 'rise_rate_one_year', 'rise_count_two_years',
                       'rise_rate_two_years']
                time.sleep(10)
                table = self.driver.find_element_by_id('tblStockList').find_elements_by_xpath(".//tr")
                print(2)
                lst = []
                for i in table[:22]:
                    lst.append(i.text.replace("\n", " ").split(' ') if re.search('^[0-9]', i.text) else '')
                df = pd.DataFrame([l for l in lst if l != ''], columns=col)
                df.date.loc[:, ] = str(self.year) + '-' + df.date.str.replace('/', '-')
                df.deal_count.loc[:, ] = df.deal_count.str.replace(',', '')
                print(3)
                # tuple_data = []
                # for t, row in df.iterrows():
                #     tuple_data.append(tuple(row))
                # insert_data(db_name, 'rise_acc', df, tuple_data)
                result = df[['rank', 'code', 'name', 'deal', 'rise_count', 'rise_rate']]
                result.columns = ['排名', '代號', '名稱', '成交', '漲跌價', '漲跌幅']
                success = True
                return result
            except:
                pass

    def html4(self):
        success = False
        while not success:
            try:
                url = "https://goodinfo.tw/StockInfo/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E5%96%AE%E6%9C%88%E7%87%9F%E6%94%B6%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E%28%E6%9C%AC%E6%9C%88%E4%BB%BD%29%40%40%E5%96%AE%E6%9C%88%E7%87%9F%E6%94%B6%40%40%E6%9C%AC%E6%9C%88%E4%BB%BD%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E"
                self.driver.get(url)
                print(1)
                col = ['rank', 'code', 'name', 'market', 'date', 'deal', 'mixed_count', 'mixed_rate', 'deal_count', 'month',
                       'mon_rev', 'mon_rev_monthly_add', 'mon_rev_monthly_add_rate', 'mon_rev_yearly_add',
                       'mon_rev_yearly_add_rate', 'acc_mon_rev', 'acc_mon_rev_yearly_add', 'acc_mon_rev_yearly_add_rate',
                       'mon_rev_bumpy']
                element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="selRPT_TIME"]')))
                dropdown = Select(element)
                dropdown.select_by_index(1)
                print(2)
                time.sleep(10)
                table = self.driver.find_element_by_id('tblStockList').find_elements_by_xpath(".//tr")
                print(3)
                lst = []
                for i in table[:22]:
                    lst.append(i.text.replace("\n", " ").split(' ') if re.search('^[0-9]', i.text) else '')
                df = pd.DataFrame([l for l in lst if l != ''], columns=col)
                df.date.loc[:, ] = str(self.year) + '-' + df.date.str.replace('/', '-')
                df.deal_count.loc[:, ] = df.deal_count.str.replace(',', '')
                df.mon_rev.loc[:, ] = df.mon_rev.str.replace(',', '')
                df.acc_mon_rev.loc[:, ] = df.acc_mon_rev.str.replace(',', '')
                print(4)
                # tuple_data = []
                # for t, row in df.iterrows():
                #     tuple_data.append(tuple(row))
                # insert_data(db_name, 'month_revenue', df, tuple_data)
                result = df[['rank', 'code', 'name', 'date', 'deal', 'mixed_count', 'mixed_rate', 'month','mon_rev']]
                result.columns = ['排名', '代號', '名稱', '日期', '成交', '漲跌價', '漲跌幅', '月份', '單月營收(億)']
                success = True
                return result
            except:
                pass

    def all_html(self):
        print('h1')
        table1 = self.html1()
        print('h2')
        table2 = self.html2()
        print('h3')
        table3 = self.html3()
        print('h4')
        table4 = self.html4()
        return table1, table2, table3, table4

class covert_to_excel():
    def __init__(self):
        pass

    def loop(table_name,table):
        date_lst = sorted(table.日期.astype(str).unique())
        writer = pd.ExcelWriter(table_name + '.xlsx', engine='xlsxwriter')
        for date in date_lst:
            subsheet = table.loc[table.日期.astype(str) == date, :]
            subsheet.to_excel(writer, sheet_name=date, startrow=1, header=False, index=False)
            workbook = writer.book
            worksheet = writer.sheets[date]
            (max_row, max_col) = subsheet.shape
            column_settings = [{'header': column} for column in table.columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
            worksheet.set_column(0, max_col - 1, 12)
        writer.save()

    def no_loop(table_name,table):
        writer = pd.ExcelWriter(table_name + '.xlsx', engine='xlsxwriter')
        subsheet = table
        subsheet.to_excel(writer, sheet_name=table_name, startrow=1, header=False, index=False)
        workbook = writer.book
        worksheet = writer.sheets[table_name]
        (max_row, max_col) = subsheet.shape
        column_settings = [{'header': column} for column in table.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        worksheet.set_column(0, max_col - 1, 12)
        writer.save()

    def all_convert(self,table1, table2, table3, table4):
        self.loop('成交額_日', table1)
        self.loop('成交額_週', table2)
        self.no_loop('累計上漲價格', table3)
        self.no_loop('月營收', table4)





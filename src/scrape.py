import time
import glob
import os
from tqdm import tqdm
import numpy as np
import threading
import datetime
from automation.custom_thread import CustomThread
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from automation.helpers import login_sanim, login_hoghogh, login_list_hoghogh, login_iris, login_arzeshafzoodeh, \
    login_tgju, get_update_date, connect_to_sql, \
    login_mostaghelat, login_codeghtesadi, login_soratmoamelat, \
    login_186, open_and_save_excel_files, \
    maybe_make_dir, input_info, merge_multiple_excel_sheets, \
    return_start_end, wait_for_download_to_finish, move_files, \
    remove_excel_files, init_driver, insert_into_tbl, \
    extract_nums, retryV1, check_driver_health, \
    log_it, is_updated_to_download, drop_into_db, unzip_files, \
    is_updated_to_save, rename_files, merge_multiple_html_files, \
    merge_multiple_excel_files, log_the_func, wrap_it_with_params, wrap_it_with_paramsv1, cleanup, wrap_a_wrapper
from automation.download_helpers import download_1000_parvandeh
from automation.constants import get_dict_years, get_sql_con, lst_years_arzeshafzoodeSonati, \
    get_soratmoamelat_mapping, return_sanim_download_links, \
    get_newdatefor186, get_url186, get186_titles, ModiHoghogh, ModiHoghoghLst, get_report_links
import threading
import math
from automation.watchdog_186 import watch_over, is_downloaded
from automation.sql_queries import get_sql_arzeshAfzoodeSonatiV2
from automation.logger import log_it
from automation.scrape_helpers import scrape_iris_helper, soratmoamelat_helper, find_obj_and_click, download_excel, \
    adam, set_hoze, set_user_permissions, set_imp_corps, select_from_dropdown, set_start_date, get_sodor_gharar_karshenasi, \
    scrape_it, insert_gashtPosti, save_process, get_amar_sodor_ray, \
    insert_sabtenamArzeshAfzoodeh, insert_codeEghtesadi, \
    scrape_1000_helper, create_1000parvande_report, \
    save_in_db, get_eghtesadidata, get_dadrasidata, get_modi_info, scrape_arzeshafzoodeh_helper, \
    get_mergeddf_arzesh, insert_arzeshafzoodelSonati, save_excel, check_health
from automation.sql_queries import get_sql_agg_soratmoamelat
from automation.soratmoamelat_helpers import get_soratmoamelat_link, \
    get_soratmoamelat_report_link, get_sorat_selected_year, get_sorat_selected_report

start_index = 1
n_retries = 0
first_list = [4, 5, 6, 7, 8, 9, 10, 21, 22, 23, 24]
second_list = [14, 15, 16, 17]
time_out_1 = 2080
time_out_2 = 2080
timeout_fifteen = 15
excel_file_names = ['Excel.xlsx', 'Excel(1).xlsx', 'Excel(2).xlsx']
badvi_file_names = ['جزئیات اعتراضات و شکایات.html']


@log_it
def log(row, success):
    print('log is called')


download_button_ezhar = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[1]/div[2]/button[3]'
download_button_ghatee_sader_shode = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[1]/div[2]/button[3]'
download_button_rest = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[1]/div[2]/button[2]'
menu_nav_1 = '//*[@id="t_MenuNav_1_1i"]'
menu_nav_2 = '/html/body/form/header/div[2]/div/ul/li[2]/div/div/div[2]/ul/li[1]/div/span[1]/a'
# menu_nav_2 = '/html/body/form/header/div[2]/div/ul/li[2]/div/div/ul/li[1]/div/span[1]/a'
year_button_1 = '//*[@id="P1100_TAX_YEAR_CONTAINER"]/div[2]/div/div'
year_button_2 = '/html/body/div[7]/div[2]/div[1]/button'
year_button_3 = '/html/body/div[6]/div[2]/div[2]/div/div[3]/ul/li'
year_button_4 = '/html/body/div[3]/div/ul/li[8]/div/span[1]/button'
switch_to_data = '/html/body/div[6]/div[2]/div[2]/div/div/div/div[2]/label/span'
download_excel_btn_1 = '/html/body/div[6]/div[2]/ul/li[1]/span[1]'
download_excel_btn_2 = '/html/body/div[6]/div[3]/div/button[2]'
input_1 = '/html/body/span/span/span[1]/input'
td_1 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]'
td_2 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div[2]/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]'
td_3 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div[2]/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]'
year_button_5 = '/html/body/div[6]/div[3]/div/button[2]'
year_button_6 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[1]/div[1]/div[3]/div/button'
td_4 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[2]/div[5]/div[1]/div/div[1]/table/tr/th[8]/a'
td_5 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div[1]/div/div/div/div[2]/div/span/span[1]/span/span[2]'
td_6 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div[1]/div/div/div/div[2]/div/span/span[1]/span/span[1]'
td_ezhar = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[%s]/a'


def retry(func):

    def try_it(Cls):
        global n_retries
        try:
            result = func(Cls)
            return result

        except Exception as e:
            n_retries += 1
            print(e)
            if n_retries < 50:
                print('trying again')
                Cls.driver.close()
                path = Cls.path
                report_type = Cls.report_type
                year = Cls.year
                time.sleep(3)
                x = Scrape(path, report_type, year)
                x.scrape_sanim()
    return try_it


def get_td_number(report_type: str) -> str:
    if (report_type == 'ezhar'):
        td_number = 4
    elif (report_type == 'hesabrasi_darjarian_before5'):
        td_number = 5
    elif (report_type == 'hesabrasi_darjarian_after5'):
        td_number = 6
    elif (report_type == 'hesabrasi_takmil_shode'):
        td_number = 7
    elif (report_type == 'tashkhis_sader_shode'):
        td_number = 8
    elif (report_type == 'tashkhis_eblagh_shode'):
        td_number = 9
    elif (report_type == 'tashkhis_eblagh_nashode'):
        td_number = 10
    elif (report_type == 'ghatee_sader_shode'):
        td_number = 21
    elif (report_type == 'ghatee_eblagh_shode'):
        td_number = 22
    elif (report_type == 'ejraee_sader_shode'):
        td_number = 23
    elif (report_type == 'ejraee_eblagh_shode'):
        td_number = 24
    elif (report_type == 'badvi_darjarian_dadrasi'):
        td_number = 15
    elif (report_type == 'badvi_takmil_shode'):
        td_number = 16
    elif (report_type == 'tajdidnazer_darjarian_dadrasi'):
        td_number = 17
    elif (report_type == 'tajdidnazar_takmil_shode'):
        td_number = 18
    return td_number


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def click_on_down_btn_sanim(driver, info, link):
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.XPATH,
                                        link))).click()
    return driver, info


@wrap_it_with_params(15, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanimforheiat(driver, info, btn_id='OBJECTION_DETAILS_IR_actions_button'):

    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located(
            (By.CLASS_NAME, 'a-IRR-actions'))).click()
    time.sleep(1)
    # WebDriverWait(driver, time_out_2).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH, year_button_4))).click()

    btns = driver.find_elements(By.TAG_NAME, 'button')

    # [btn.click() if btn.text == 'دانلود کردن' else continue for btn in btns]
    for btn in btns:
        if btn.text == 'دانلود کردن':
            btn.click()
            break

    time.sleep(0.5)
    if btn_id == "OBJECTION_DETAILS_IR_actions_button":
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, switch_to_data))).click()
        time.sleep(0.5)
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, download_excel_btn_1))).click()
    else:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, '/html/body/div[7]/div[2]/div[2]/div/div/div/div[2]/label/span'))).click()
        time.sleep(1)
        btns = driver.find_element(By.XPATH, '/html/body/div[7]/div[2]/ul')
        btns = btns.find_elements(By.TAG_NAME, 'li')
        for btn in btns:
            if btn.text == 'HTML':
                btn.click()
                break
        # WebDriverWait(driver, 3).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[7]/div[2]/ul/li'))).click()
    time.sleep(0.5)

    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanimforheiatend(driver, info, xpath=download_excel_btn_2):
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).click()
    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanim(driver, info):

    elms = driver.find_elements(By.TAG_NAME, 'strong')
    is_found = False

    for elm in elms:
        if elm.text in [
            'گزارش اظهارنامه ها',
            'گزارش شکایات بدوی در جریان دادرسی',
            'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان -  مالیات بر درآمد شرکت ها',
            'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
            'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
            'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
            'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
            'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
            'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
            'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
            'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
            'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
            'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
            'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
            'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
        ]:
            is_found = True
            break

    if not is_found:
        raise Exception

    btns = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.XPATH,
                                        "/html/body/form/div[2]/div/div[2]/main/div[2]/div")))

    elms = btns.find_elements(By.TAG_NAME, 'button')

    [elm.click() for elm in elms if elm.text == 'Excel']

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def list_details(driver=None, info=None, report_type='ezhar', manba='hoghoghi'):
    if (report_type == 'tashkhis_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]\
                    /main/div[2]/div/div/div/div/font\
                    /div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/\
                    table/tbody/tr[2]/td[14]/a'

    elif (report_type == 'tashkhis_eblagh_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/\
                    div/div/div/font/div/div/div[2]/div[2]/\
                    div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[4]/a'

    elif (report_type == 'ezhar' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/\
                            main/div[2]/div/div/div/div/div/\
                            div/div[2]/div[2]/div[5]/div[1]/div/div[2]/\
                            table/tbody/tr[2]/td[5]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/\
                            div/div/div/div/font/div/div/div[2]/div[2]/\
                            div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[14]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'haghighi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/\
                            div/div/div/div/font/div/div/div[2]/\
                            div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[15]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'arzesh'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]\
                            /div/div/div/div/font/div/div/div[2]/div[2]/div[5]\
                            /div[1]/div/div[3]/table/tbody/tr[2]/td[8]/a'

    WebDriverWait(driver, timeout_fifteen).until(
        EC.presence_of_element_located(
            (By.XPATH, info['link_list']))).click()

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_btn_type(driver=None,
                    info=None,
                    report_type=None):

    if report_type == 'ezhar':
        download_button = download_button_ezhar
    elif report_type == 'ghatee_sader_shode':
        download_button = download_button_ghatee_sader_shode
    else:
        download_button_rest
    WebDriverWait(driver, time_out_2).until(
        EC.presence_of_element_located(
            (By.XPATH, download_button))).click()
    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_year(driver, info={}, year=None):

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.ID, 'P1100_TAX_YEAR'))).click()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[6]/div[2]/div[1]/input'))).clear()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[6]/div[2]/div[1]/input'))).send_keys(
        year)

    driver.find_element(
        By.XPATH, '/html/body/div[6]/div[2]/div[1]/input').send_keys(Keys.ENTER)

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/div[6]/div[2]/div[1]/button'))).click()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, year_button_3))).click()

    while (year != WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/form/div[2]/div/div[2]/main/\
            div[2]/div/div/div/div/font/div[2]/div\
            /div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]'))).text):
        time.sleep(1)
        print('waiting for the year to be selected')

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_column(driver, info={}, td_number=None):
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, '%s/td[4]/a' % td_2)))
    driver.find_element(By.XPATH, '%s/td[%s]/a' %
                        (td_2, td_number)).click()
    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def get_main_menu(driver, info={}):
    WebDriverWait(driver, 24).until(
        EC.presence_of_element_located((By.XPATH,
                                        '/html/body/form/header/div[2]/div/ul/li[2]/span/span'))).click()
    time.sleep(1)
    WebDriverWait(driver, 24).until(
        EC.presence_of_element_located((By.XPATH,
                                        '//*[@id="t_MenuNav_1_0i"]'))).click()
    return driver, info


class Scrape:

    def __init__(self,
                 path=None,
                 report_type=None,
                 year=None,
                 driver_type='firefox',
                 headless=True,
                 info={},
                 driver=None,
                 time_out=180,
                 lock=None,
                 table_name=None,
                 type_of=None):
        self.path = path
        self.report_type = report_type
        self.year = year
        self.driver_type = driver_type
        self.headless = headless
        self.info = info
        self.driver = driver
        self.time_out = time_out
        self.lock = lock
        self.table_name = table_name,
        self.type_of = type_of

        if isinstance(self.table_name, tuple):
            self.table_name = self.table_name[0]

    @log_the_func('none')
    def scrape_186(self, path=None, return_df=True, headless=False, fromdate='13910101',
                   todate='14040101', *args, **kwargs):
        remove_excel_files(file_path=path,
                           postfix=['.xls', '.html', 'xlsx'])
        self.driver = init_driver(pathsave=path,
                                  driver_type=self.driver_type,
                                  headless=headless)
        self.path = path
        self.driver = login_186(self.driver)
        time.sleep(3)
        titles = get186_titles()

        for name in titles:
            done = False
            date_downloaded = False
            dates = get_newdatefor186()
            fromdate, todate = next(dates)
            while not done:
                try:
                    if date_downloaded:
                        fromdate, todate = next(dates)
                        date_downloaded = False
                except Exception as e:
                    print(e)
                    done = True
                    # Move downloaded files
                    dir_to_move = os.path.join(path, name)
                    maybe_make_dir([dir_to_move])
                    srcs = glob.glob(path + "/*" + 'xlsx')
                    dsts = []
                    rename_files('path', path, file_list=srcs, postfix='.xlsx')
                    srcs = glob.glob(path + "/*" + 'xlsx')
                    for i in range(len(srcs)):
                        dsts.append(dir_to_move)
                    move_files(srcs, dsts)

                    continue
                url = get_url186(name, fromdate, todate)
                self.driver.get(url)
                try:
                    t1 = threading.Thread(
                        target=save_process, args=(self.driver, self.path))
                    t2 = threading.Thread(
                        target=watch_over, args=(self.path, 240, 2))
                    t1.start()
                    t2.start()
                    t1.join()
                    t2.join()
                    wait_for_download_to_finish(path, ['xls'])
                    time.sleep(3)
                    date_downloaded = True
                except Exception as e:
                    date_downloaded = False

        scrape186_helper(url)

    def scrape_tgju(self, path=None, return_df=True):
        self.driver = init_driver(pathsave=path,
                                  driver_type=self.driver_type,
                                  headless=True)
        self.path = path
        self.driver = login_tgju(self.driver)
        WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span')))
        price = self.driver.find_element(
            By.XPATH,
            '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span').text
        WebDriverWait(self.driver, 540).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 '/html/body/div[2]/header/div[2]/div[6]/ul/li/a/img')))
        try:
            if (self.driver.find_element(
                    By.XPATH,
                    '/html/body/div[2]/header/div[2]/div[6]/ul/li/a/img')):
                time.sleep(5)
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span'
                    )))
                coin = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span'
                ).text

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[6]/span[1]/span'
                    )))
                dollar = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[6]/span[1]/span'
                ).text

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[4]/span[1]/span'
                    )))
                gold = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[4]/span[1]/span'
                ).text
        except Exception as e:
            print(e)

        self.driver.close()

        return coin, dollar, gold

    def scrape_soratmoamelat(self, path=None, return_df=False):

        def scrape_it():
            if del_prev_files:
                remove_excel_files(file_path=path,
                                   postfix=['.xls', '.html', 'xlsx'])
            self.driver = init_driver(pathsave=path,
                                      driver_type=self.driver_type,
                                      headless=headless)
            self.path = path
            self.driver = login_soratmoamelat(self.driver)
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]')))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
                )))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
            ).click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0').click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3').click()

            def arzesh(i):
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_frm_year')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_frm_year'))
                sel.select_by_index(i)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_frm_period')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_frm_period'))
                sel.select_by_index(0)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_To_year')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_To_year'))
                sel.select_by_index(i)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_To_period')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_To_period'))
                sel.select_by_index(3)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_Button3')))
                time.sleep(10)
                self.driver.find_element(
                    By.ID, 'ctl00_ContentPlaceHolder1_Button3').click()

    # @retryV1
    # @log_the_func('none')
    @wrap_it_with_params(10, 10, True, False, False, False)
    def scrape_codeghtesadi(self,
                            path=None,
                            return_df=True,
                            data_gathering=False,
                            pred_captcha=False,
                            codeeghtesadi={
                                'state': True,
                                'params': {'set_important_corps': True,
                                           'getdata': False,
                                           'adam': False,
                                           'df_toadam': None,
                                           'del_prev_files': True,
                                           'merge': False,
                                           'get_info': False,
                                           'saving_dir': None,
                                           }},
                            df_toadam=None,
                            soratmoamelat={'state': True,
                                           'params': {'scrape': {'general': True, 'gomrok': True,
                                                                 'sorat': {'scrape': True, 'years': ['1397']},
                                                                 'sayer': False},

                                                      'unzip': False,
                                                      'dropdb': False,
                                                      'gen_report': True}},
                            headless=False,
                            *args,
                            **kwargs):

        self.path = path

        if soratmoamelat['state']:
            if soratmoamelat['params']['scrape']['general']:
                self.driver, self.info = scrape_it(
                    path, self.driver_type, headless=headless, driver=self.driver, info=self.info)
                self.driver, self.info = get_soratmoamelat_link(
                    driver=self.driver, info=self.info)

                self.driver, self.info = get_soratmoamelat_report_link(
                    driver=self.driver, info=self.info)

                if soratmoamelat['params']['scrape']['sorat']['scrape']:
                    for year in soratmoamelat['params']['scrape']['sorat']['years']:
                        self.driver, self.info = get_sorat_selected_year(
                            driver=self.driver, info=self.info, year=year)
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlTTMSCategory'))

                        options_count = len(sel.options) - 1
                        for i in range(101, 113):
                            time.sleep(2)

                            self.driver, self.info = get_sorat_selected_year(
                                driver=self.driver, info=self.info, year=year)
                            time.sleep(1)
                            self.driver, self.info = get_sorat_selected_report(
                                driver=self.driver, info=self.info, index=i)
                            time.sleep(1)
                            tbl_name = get_soratmoamelat_mapping()[
                                self.info['selected_option_text']]
                            tbl_name = tbl_name + \
                                '%s' % self.info['selected_year_text']

                            field = kwargs['field'] if 'field' in kwargs else None
                            soratmoamelat_helper(driver=self.driver,
                                                 info=self.info,
                                                 path=self.path,
                                                 table_name=tbl_name,
                                                 report_type='sorat',
                                                 selected_option_text=self.info['selected_option_text'],
                                                 index=i,
                                                 field=field)

                if soratmoamelat['params']['scrape']['gomrok']:
                    self.driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[1]/td[2]/div/div/div/div/div/ul/li[2]/a').click()
                    sel = Select(
                        self.driver.find_element(
                            By.ID, 'CPC_Remained_Str_Rem_ddlCuCategory'))
                    for i in range(10, 12):
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlCuCategory'))
                        selected_option = sel.select_by_value(str(i))
                        selected_option_text = sel.first_selected_option.text
                        tbl_name = get_soratmoamelat_mapping()[
                            selected_option_text]

                        soratmoamelat_helper(self.driver, self.path,
                                             tbl_name, selected_option_text=selected_option_text, report_type='gomrok', index=i, field=kwargs['field'])

                if soratmoamelat['params']['scrape']['sayer']:
                    self.driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[1]/td[2]/div/div/div/div/div/ul/li[3]/a').click()
                    sel = Select(
                        self.driver.find_element(
                            By.ID, 'CPC_Remained_Str_Rem_ddlExtCategory'))
                    for i in [800, 970, 830, 750, 980, 990, 640, 820, 900,
                              650, 69, 71, 70, 72, 270, 260, 680, 960, 660, 670]:
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlExtCategory'))
                        selected_option = sel.select_by_value(str(i))
                        selected_option_text = sel.first_selected_option.text
                        tbl_name = get_soratmoamelat_mapping()[
                            selected_option_text]

                        soratmoamelat_helper(self.driver, self.path,
                                             tbl_name, selected_option_text=selected_option_text, report_type='sayer', index=i, field=kwargs['field'])
                self.driver.close()

                # except Exception as e:
                #     return (self.driver, e)

            if soratmoamelat['params']['unzip']:
                unzip_files(self.path)

            if soratmoamelat['params']['dropdb']:
                save_in_db(
                    self.path, soratmoamelat['params']['scrape']['sorat']['years'][0])

            if soratmoamelat['params']['gen_report']:
                connect_to_sql(get_sql_agg_soratmoamelat(), sql_con=get_sql_con(
                    database='testdb'), num_runs=0)

        if codeeghtesadi['state']:

            if codeeghtesadi['params']['set_important_corps']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                set_imp_corps(
                    self.driver, codeeghtesadi['params']['df'], codeeghtesadi['params']['saving_dir'])

            if codeeghtesadi['params']['adam']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                adam(self.driver, codeeghtesadi['params']['df_toadam'])

            if codeeghtesadi['params']['set_hoze']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                set_hoze(self.driver, codeeghtesadi['params']['df_set_hoze'])

            if codeeghtesadi['params']['set_user_permissions']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                set_user_permissions(
                    self.driver, codeeghtesadi['params']['df'])

            if codeeghtesadi['params']['getdata']:
                with init_driver(
                        pathsave=path, driver_type=self.driver_type, headless=headless, info=self.info) as self.driver:
                    path = path
                    self.driver, self.info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
                # self.driver = scrape_it(path=path, driver_type=self.driver_type,
                #                         headless=headless, data_gathering=data_gathering, pred_captcha=pred_captcha)
                    get_eghtesadidata(self.driver, self.path,
                                      del_prev_files=codeeghtesadi['params']['del_prev_files'])

            if codeeghtesadi['params']['get_dadrasi']:
                with init_driver(
                        pathsave=path, driver_type=self.driver_type,
                        headless=headless, info=self.info, prefs={'maximize': True,
                                                                  'zoom': '0.8'}) as self.driver:
                    path = path
                    self.driver, self.info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info,
                        user_name='1756914443', password='1756914443')
                # self.driver = scrape_it(path=path, driver_type=self.driver_type,
                #                         headless=headless, data_gathering=data_gathering, pred_captcha=pred_captcha)
                    get_dadrasidata(self.driver, self.path,
                                    del_prev_files=codeeghtesadi['params']['del_prev_files'], info=self.info)

            if codeeghtesadi['params']['get_info']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                get_modi_info(self.driver, self.path,
                              codeeghtesadi['params']['df'], codeeghtesadi['params']['saving_dir'])

            if codeeghtesadi['params']['merge']:
                merge_multiple_excel_sheets(path,
                                            dest=path,
                                            table='tblCodeeghtesadi',
                                            multithread=True)
        return self.driver, self.info

    def scrape_mostaghelat(self,
                           path=None,
                           scrape=False,
                           read_from_repo=True,
                           report_type='Tashkhis',
                           return_df=False,
                           table_name=None,
                           drop_to_sql=False,
                           append_to_prev=False,
                           del_prev_files=True,
                           db_name='testdbV2',
                           headless=False):

        # Scrape process
        def scrape_it():
            with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless) as self.driver:
                self.path = path
                self.driver, self.info = login_mostaghelat(self.driver)

                @wrap_it_with_params(50, 1000000000, False, True, True, False)
                def select_menu(driver, info):
                    WebDriverWait(self.driver, 66).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             '/html/body/form/div[4]/div[1]/ul[1]/li[10]/a/span'))).click()
                    return self.driver, self.info

                self.driver, self.info = select_menu(
                    driver=self.driver, info=self.info)
                time.sleep(1)
                if report_type == 'AmadeGhatee':
                    index = '11'
                    select_type = 'Dro_S_TaxOffice'
                elif report_type == 'Tashkhis':
                    index = '3'
                    select_type = 'Drop_S_TaxUnitCode'
                elif report_type == 'Ghatee':
                    path_second_date = '/html/body/form/div[4]/div[2]/div/div[2]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[3]/td[4]/button'
                    index = '4'
                    select_type = 'Drop_S_TaxUnitCode'
                elif report_type == 'Ezhar':
                    path_second_date = '/html/body/form/div[4]/div[2]/div/div[2]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[3]/td[4]/button'
                    index = '2'

                WebDriverWait(self.driver, 24).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/form/div[4]/div[1]/ul[1]/li[10]/ul/li[%s]/a/i[2]'
                        % index))).click()

                if report_type == 'Ezhar':
                    Select(self.driver.find_element(
                        By.ID, 'Drop_S_Year')).select_by_value('1401')

                    dict_ezhar = {'residegi_nashode': '1',
                                  'residegi_shode': '2'}
                    prev_text = ''
                    for key, value in dict_ezhar.items():

                        # select ezhar residegi_nashode
                        Select(self.driver.find_element(
                            By.ID, 'Drop_S_PossessionName')).select_by_value(value)

                        time.sleep(3)

                        WebDriverWait(self.driver, 24).until(
                            EC.presence_of_element_located((
                                By.ID,
                                'Btn_Search'))).click()

                        @wrap_it_with_params(50, 1000000000, False, True, True, False)
                        def wait_for_res(driver, info={}, prev_text=''):
                            info['text'] = WebDriverWait(driver, 1).until(
                                EC.presence_of_element_located((
                                    By.ID,
                                    'ContentPlaceHolder1_Lbl_Count'))).text
                            while (prev_text == info['text']):
                                raise Exception
                            return driver, info

                        self.driver, self.info = wait_for_res(
                            driver=self.driver, info=self.info, prev_text=prev_text)
                        prev_text = self.info['text']

                        time.sleep(1)

                        def down(driver, path):

                            WebDriverWait(driver, 24).until(
                                EC.presence_of_element_located((
                                    By.ID,
                                    'ContentPlaceHolder1_Btn_Export'))).click()

                        try:
                            t1 = threading.Thread(
                                target=down, args=(self.driver, self.path))
                            t2 = threading.Thread(
                                target=watch_over, args=(self.path, 240, 2))
                            t1.start()
                            t2.start()
                            t1.join()
                            t2.join()
                            wait_for_download_to_finish(self.path, ['xls'])
                            time.sleep(3)
                            date_downloaded = True

                        except Exception as e:
                            date_downloaded = False

                    self.driver.close()
                    return

                if select_type == 'Drop_S_TaxUnitCode':
                    time.sleep(3)
                    WebDriverWait(self.driver, 48).until(
                        EC.presence_of_element_located(
                            (By.ID, 'Txt_RegisterDateAz')))
                    # time.sleep(5)
                    self.driver.find_element(
                        By.ID, 'Txt_RegisterDateAz').click()
                    time.sleep(1)
                    sel = Select(
                        self.driver.find_element(By.ID,
                                                 'bd-year-Txt_RegisterDateAz'))
                    sel.select_by_index(0)
                    WebDriverWait(self.driver, 24).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'day-1')))
                    self.driver.find_element(By.CLASS_NAME, 'day-1').click()

                    WebDriverWait(self.driver, 24).until(
                        EC.presence_of_element_located(
                            (By.ID, 'Txt_RegisterDateTa')))
                    self.driver.find_element(
                        By.ID, 'Txt_RegisterDateTa').click()
                    sel = Select(
                        self.driver.find_element(By.ID,
                                                 'bd-year-Txt_RegisterDateTa'))
                    sel.select_by_index(99)
                    if report_type == 'Tashkhis':
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located((
                                By.XPATH,
                                '/html/body/form/div[4]/div[2]/div/div[2]/div/div/div[2]/div/div/\
                                    div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[1]/td[7]/button'
                            ))).click()

                    else:
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located(
                                (By.XPATH, path_second_date)))
                        self.driver.find_element(By.XPATH,
                                                 path_second_date).click()

                if report_type == 'amade_ghatee':

                    sel = Select(self.driver.find_element(By.ID, select_type))
                    count = len(sel.options) - 1
                else:
                    count = 1

                def mostagh(i, select_type=select_type):
                    try:
                        if report_type == 'amade_ghatee':
                            sel = Select(
                                self.driver.find_element(By.ID, select_type))
                            sel.select_by_index(i)

                        sel = Select(
                            self.driver.find_element(By.ID, 'Drop_S_TypeAnnunciation'))
                        sel.select_by_index(i)

                        WebDriverWait(self.driver, 4).until(
                            EC.presence_of_element_located((By.ID, 'Btn_Search')))
                        self.driver.find_element(By.ID, 'Btn_Search').click()
                        if_true = self.driver.find_element(
                            By.ID, 'ContentPlaceHolder1_Lbl_Count').text != 'تعداد : 0 مورد'
                        if report_type == 'amade_ghatee':
                            try:
                                if (self.driver.find_element(
                                        By.ID, 'ContentPlaceHolder1_Btn_Export')):
                                    time.sleep(4)
                                    self.driver.find_element(
                                        By.ID,
                                        'ContentPlaceHolder1_Btn_Export').click()
                            except Exception as e:
                                global start_index
                                start_index += 1
                                mostagh(start_index, select_type=select_type)

                        elif (if_true):
                            try:
                                if (self.driver.find_element(
                                        By.ID, 'ContentPlaceHolder1_Btn_Export')):
                                    time.sleep(4)
                                    self.driver.find_element(
                                        By.ID,
                                        'ContentPlaceHolder1_Btn_Export').click()
                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)
                        return

                global start_index
                while start_index <= count:
                    for i in range(2):
                        try:
                            t1 = threading.Thread(
                                target=mostagh, args=(i, ))
                            t2 = threading.Thread(target=watch_over,
                                                  args=(self.path, 15))
                            t1.start()
                            t2.start()
                            t1.join()
                            t2.join()
                            start_index += 1
                            wait_for_download_to_finish(
                                self.path, ['xls', 'xlsx'])
                        except Exception as e:
                            print(e)
                            continue
                start_index = 1
                wait_for_download_to_finish(self.path, ['xls', 'xlsx'])

        if scrape:
            scrape_it()

            df = merge_multiple_excel_files(path,
                                            path,
                                            table_name=table_name,
                                            delete_after_merge=True,
                                            postfix='xls',
                                            return_df=True)

        if read_from_repo:
            dest = os.path.join(path, 'merged', table_name + '.xlsx')
            df = pd.read_excel(dest)
            df = df.astype(str)

        if drop_to_sql:
            drop_into_db(table_name=table_name,
                         columns=df.columns.tolist(),
                         values=df.values.tolist(),
                         append_to_prev=append_to_prev,
                         db_name=db_name)

        if return_df:
            return df

    @log_the_func('none')
    def scrape_arzeshafzoodeh(self,
                              path=None,
                              return_df=True,
                              del_prev_files=True,
                              headless=False,
                              scrape=False,
                              merge_df=False,
                              inser_to_db=True,
                              insert_sabtenamArzesh=False,
                              insert_codeEghtesad=False,
                              insert_gash=False,
                              *args,
                              **kwargs):

        if scrape:
            scrape_arzeshafzoodeh_helper(
                path=path, del_prev_files=del_prev_files, headless=headless, field=kwargs['field'])

        # Move the files to the temp folder
        if merge_df:
            df_arzesh = get_mergeddf_arzesh(path, return_df=True)
        else:
            df_arzesh = None

        if inser_to_db:
            insert_arzeshafzoodelSonati(df_arzesh=df_arzesh)

        if insert_sabtenamArzesh:
            insert_sabtenamArzeshAfzoodeh()

        if insert_codeEghtesad:
            insert_codeEghtesadi()

        if insert_gash:
            insert_gashtPosti()

        insert_into_tbl(sql_query=get_sql_arzeshAfzoodeSonatiV2(),
                        tbl_name='tbl_ArzeshAfzoodeSonati', convert_to_str=True)

        # df_merged_1 = df_merged.merge(df_codeeghtesadi, how='left',
        #                               left_on='کدرهگیری', right_on='کد رهگیری')

        # Scrape contents of arzesh afzoode from the website
        # scrape_it()
        time.sleep(2)
        # Merge files and return df
        # df_merged = get_mergeddf()

        return driver, info

    def scrape_1000parvande(self, path, scrape=True,
                            headless=False,
                            del_prev_files=True,
                            merge=True,
                            create_report=True,
                            open_and_save_excel=True,
                            *args, **kwargs):
        if del_prev_files:
            remove_excel_files(file_path=path,
                               postfix=['.xls', '.html', 'xlsx'])

        if scrape:
            self.driver = init_driver(pathsave=path,
                                      driver_type=self.driver_type,
                                      headless=headless)
            self.driver, self.info = login_sanim(
                driver=self.driver, info=self.info)

            scrape_1000_helper(self.driver)

        if open_and_save_excel:

            open_and_save_excel_files(path=path)

        if merge:

            df = merge_multiple_excel_files(path=path,
                                            dest=path,
                                            delete_after_merge=False,
                                            return_df=True)

        if create_report:
            create_1000parvande_report(path, 'finals.xlsx', df, path)

    # @retry
    @wrap_it_with_params(50, 1000000000, False, True, True, False)
    def scrape_sanim(self, *args, **kwargs):
        with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as self.driver:
            self.driver, self.info = login_sanim(
                driver=self.driver, info=self.info)
            links = return_sanim_download_links(
                self.info['cur_instance'], self.report_type, self.year)
            for link in links:
                self.driver.get(link)
            if self.report_type not in ['badvi_darjarian_dadrasi',
                                        'badvi_takmil_shode',
                                        'tajdidnazer_darjarian_dadrasi',
                                        'tajdidnazar_takmil_shode',
                                        'badvi_darjarian_dadrasi_hamarz',
                                        'amar_sodor_gharar_karshenasi',
                                        'amar_sodor_ray']:
                download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)
            else:
                btn_id = 'OBJECTION_DETAILS_IR_actions_button'
                xpath_down = download_excel_btn_2

                if self.report_type == 'amar_sodor_ray':
                    btn_id = 'TaxOffice_Income_actions_button'
                    xpath_down = '/html/body/div[7]/div[3]/div/button[2]'
                    self.driver, self.info = get_amar_sodor_ray(
                        driver=self.driver, info=self.info)

                if self.report_type == 'amar_sodor_gharar_karshenasi':
                    btn_id = 'TaxOffice_Income_actions_button'
                    xpath_down = '/html/body/div[7]/div[3]/div/button[2]'
                    self.driver, self.info = get_sodor_gharar_karshenasi(
                        driver=self.driver, info=self.info)

                self.driver, self.info = click_on_down_btn_excelsanimforheiat(
                    driver=self.driver, info=self.info, btn_id=btn_id)
                download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info, xpath=xpath_down),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)

        return self.driver, self.info

    def scrape_hoghogh(self, path=None, return_df=False, del_prev_files=True):

        def scrape_it():
            if del_prev_files:
                remove_excel_files(file_path=path,
                                   postfix=['.xls', '.html', 'xlsx'])
            self.driver = init_driver(pathsave=path,
                                      driver_type=self.driver_type,
                                      headless=headless)
            self.path = path
            self.driver = login_hoghogh(self.driver)
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]')))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
                )))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
            ).click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0'))).click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2'))).click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3'))).click()

        scrape_it()

    @wrap_a_wrapper
    @wrap_it_with_params(50, 1000000000, False, True, False, False)
    def scrape_list_hoghogh(self, path=None, headless=True, creds={}, *args, **kwargs):
        self.driver = init_driver(pathsave=path,
                                  driver_type=self.driver_type,
                                  headless=headless)

        self.driver = login_list_hoghogh(self.driver, creds=creds)

        WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH, '/html/body/div[2]/div[1]/nav/ul/li[3]/a'))).click()
        time.sleep(1)
        # WebDriverWait(self.driver, 8).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[2]/div[2]/div/div[3]/fieldset/table/tbody/tr[2]/td[10]/div[1]/a'))).click()
        # WebDriverWait(self.driver, 8).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[2]/div[2]/form/fieldset/a[4]'))).click()
        # sel = Select(
        #     self.driver.find_element(
        #         By.NAME, 'hoghoughtable_length'))
        # sel.select_by_index(3)
        text_info = WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'karmandstable_info'))).text
        nums = extract_nums(text_info, expression=r'(\d+,\d+)')
        nums = extract_nums(nums[0])
        num = ''
        for item in nums:
            num += item
        num = math.ceil(int(num) / 10)
        lst_modis = []
        for i in range(num-1):

            table = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'karmandstable')))

            rows = table.find_elements(By.TAG_NAME, 'tr')

            for row in rows[1:]:
                tds = row.find_elements(By.TAG_NAME, 'td')[:-1]

                modi = ModiHoghoghLst()
                modi.melli_code = tds[0].text
                modi.name = tds[1].text
                modi.sur_name = tds[2].text
                modi.position = tds[3].text
                lst_modis.append([modi.melli_code,
                                  modi.name,
                                  modi.sur_name,
                                  modi.position,
                                  ])

                # Go to next page

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'karmandstable_next'))).click()
            time.sleep(1)

        print('f')

        return self.driver

    # @wrap_a_wrapper
    @wrap_it_with_paramsv1(50, 1000000000, True, True, True, True)
    def scrape_iris(self,
                    path=None,
                    df=None,
                    return_df=False,
                    del_prev_files=True,
                    headless=True,
                    file_name='file.xlsx',
                    backup_file='file.xlsx',
                    time_out=130,
                    role='manager_phase2',
                    end_time=19,
                    save_every_main=2,
                    save_every_back=10,
                    shenase_dadrasi='no',
                    info={}):
        # print(df[1]['user'].head(1).item())
        # try:

        if df is None:
            data_file_path = r'C:\Users\alkav\Documents\file.xlsx'
            users_file_path = r'C:\Users\alkav\Documents\users.xlsx'
            df_data = pd.read_excel(
                data_file_path)
            df_users = pd.read_excel(users_file_path)

            df = [df_data, df_users]

            if (df[0].loc[df[0]['is_done'].isna()].empty):
                return

        if del_prev_files:
            remove_excel_files(file_path=path,
                               postfix=['.xls', '.html', 'xlsx'])
        self.info = info
        self.path = path

        self.driver, self.info = init_driver(pathsave=path,
                                             driver_type=self.driver_type,
                                             headless=headless, prefs={'maximize': True,
                                                                       'zoom': '0.73'}, info=self.info)
        self.driver, self.info = login_iris(self.driver, creds={'username': str(df[1][role].iloc[0]),
                                                                'pass': str(df[1]['pass'].iloc[0])}, info=self.info)
        # self.driver = login_iris(self.driver, creds={'username': df[1][role].loc[2],
        #                                              'pass': str(df[1]['pass'].loc[2].item())})
        # self.driver, self.info = check_health(
        #     self.driver, init=True, info=self.info)

        # if not self.info['success']:
        #     raise Exception

        self.driver, self.info = find_obj_and_click(
            driver=self.driver, info=self.info)

        # if not self.info['success']:
        #     raise Exception

        time.sleep(3)

        if (role == 'manager_phase2'):
            df_final = df[0].loc[(df[0]['second_phase'] == 'yes') & (
                df[0]['shenase_dadrasi'].isna())]
        elif (role == 'manager_phase1'):
            df_final = df[0]
        else:
            df_final = df[0].loc[(df[0]['success'] == 'yes') & (df[0]['second_phase'].isna()) & (
                df[0]['vaziat'] == 'ثبت شده توسط خدمات مودیان')]
            # df_final = df[0].loc[df[0]['second_phase'].isna()]
            # df_final = df[0].loc[df[0]['vaziat'] =='ثبت شده توسط خدمات مودیان']
        if not df_final.empty:
            for index, item in tqdm(df_final.iterrows()):
                cur_time = datetime.datetime.now().hour
                if cur_time == end_time:
                    self.driver, self.info = cleanup(
                        driver=self.driver, info=self.info)
                    return self.driver
                stop_threads = False

                t = CustomThread(target=scrape_iris_helper, args=(
                    lambda: stop_threads, index, item, self.driver, df, file_name, backup_file,
                    role, save_every_main, save_every_back, shenase_dadrasi))

                # t = CustomThread(target=scrape_iris_helper, kwargs={
                #     'stop_threads': stop_threads, 'index': index, 'item': item, 'driver': self.driver,
                #     'df': df, 'file_name': file_name, 'backup_file': backup_file,
                #     'role': role, 'save_every_main': save_every_main, 'save_every_back': save_every_back,
                #     'shenase_dadrasi': shenase_dadrasi, 'info': info})

                t.start()
                res = t.join(time_out)

                if res is None:

                    self.driver, self.info = cleanup(
                        self.driver, info=self.info, close_driver=True)
                    self.driver, self.info = save_excel(index, df, file_name, backup_file,
                                                        'yes', 'no', driver=self.driver)
                    t.kill()
                    res = t.join()
                    self.driver, self.info = init_driver(pathsave=path,
                                                         driver_type=self.driver_type,
                                                         headless=headless, prefs={'maximize': True,
                                                                                   'zoom': '0.73'}, info=self.info)
                    self.driver, self.info = login_iris(driver=self.driver, creds={'username': df[1][role].head(1).item(),
                                                                                   'pass': df[1]['pass'].head(1).item()}, info=self.info)
                    self.driver, self.info = find_obj_and_click(
                        driver=self.driver, info=self.info)

                if self.driver is None:
                    return self.driver

        return self.driver, self.info

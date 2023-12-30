import time
import glob
import os
import tqdm
import numpy as np
import pandas as pd
import pickle
from urllib.parse import unquote
import math
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from automation.design_patterns.strategy_pattern.check_health import CheckHealthOne, CheckHealthTWO, CheckHealthThree, CheckHealth
from automation.helpers import login_sanim, login_arzeshafzoodeh, login_tgju, extract_nums, wrap_a_wrapper, login_iris, \
    login_mostaghelat, login_codeghtesadi, login_soratmoamelat, wrap_it_with_paramsv1, wrap_it_with_params, \
    maybe_make_dir, input_info, merge_multiple_excel_sheets, return_start_end, connect_to_sql, \
    wait_for_download_to_finish, move_files, log_the_func, df_to_excelsheet, \
    remove_excel_files, init_driver, insert_into_tbl, extract_nums, list_files, \
    log_it, is_updated_to_download, drop_into_db, time_it, check_update, get_update_date, \
    is_updated_to_save, rename_files, merge_multiple_html_files, merge_multiple_excel_files
from automation.download_helpers import download_1000_parvandeh
from automation.constants import get_dict_years, lst_years_arzeshafzoodeSonati, get_soratmoamelat_mapping, Modi, Modi_pazerande, get_soratmoamelat_mapping_address
import threading
from automation.watchdog_186 import watch_over, is_downloaded
from automation.sql_queries import get_sql_arzeshAfzoodeSonatiV2
from automation.logger import log_it
from automation.soratmoamelat_helpers import *


@log_it
def log(row, success):
    print('log is called')


@log_the_func('none', soratmoamelat=True)
@check_update
@time_it(log=True, db={'db_name': 'testdbV2', 'tbl_name': 'tblLog', 'append_to_prev': True})
def soratmoamelat_helper(driver=None, info={}, path=None, table_name=None, report_type=None,
                         selected_option_text=None, index=1,
                         accepted_number=10000, *args, **kwargs):

    selected_option_text = get_soratmoamelat_mapping(
    )[selected_option_text.split('\\')[-1]]

    remove_excel_files(file_path=os.path.join(
        path, selected_option_text), postfix=['xlsx', 'zip'])
    try:
        if report_type == 'gomrok':
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnCu'

        elif report_type == 'sayer':
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div[2]/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnExt'

        else:
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnTTMS'

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, btn_search))).click()

        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((
                By.XPATH,
                count_loc
            )))
        count = driver.find_element(
            By.XPATH,
            count_loc
        ).text
        count = count.replace(',', '')
        count = int(extract_nums(count)[0])

    except Exception as e:

        try:
            count = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]/span/b'
            ).text
            count = int(extract_nums(count)[0])
        except Exception as e:
            try:
                count = driver.find_element(
                    By.XPATH,
                    count_loc
                ).text
                count = int(extract_nums(count)[0])
            except Exception as e:
                count = 0
                driver = back_it(driver)
                return

    if count < accepted_number:
        driver = if_less_then_down(
            driver, selected_option_text, path, report_type, index=index, field=kwargs['field'])
        driver = back_it(driver)

    else:

        price_ranges = return_start_end(0, 1000000000000000, 1000000)

        for price in price_ranges:
            start_p = price[0]
            end_p = price[1]
            driver, info = recur_down(driver=driver, info=info,
                                      start_p=start_p, end_p=end_p, index=index)
            if info['count'] == 0:
                back_it()
                break
            elif info['count'] > accepted_number:
                while count > accepted_number:
                    start_p, down_count, driver = recur_until_less(
                        start_p, end_p, count, selected_option_text, index, accepted_number, driver, path, field=kwargs['field'])
                    info['count'] -= down_count

                _, _, driver = recur_until_less(start_p, end_p, count,
                                                selected_option_text, index, accepted_number, driver, path, field=kwargs['field'])

            elif (info['count'] != 0 and info['count'] < accepted_number):
                driver, info = if_less_then_down(
                    driver=driver, info=info, name=selected_option_text, path=path,
                    report_type=report_type, index=index, field=kwargs['field'])


def adam(driver, df):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(1)
    for index, row in df.iterrows():
        try:
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[1]/input"
            ).send_keys(str(row["rahgiri"]))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span"
            ).click()

            if driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span"
            ).text == "غيرفعال":
                log(row["rahgiri"], 'قبلا غیر فعال شده است')
                continue

            time.sleep(1)
            try:
                if (driver.find_element(
                        By.XPATH,
                        "//span[contains(text(),'نمایش شناسنامه')]")):
                    driver.find_element(
                        By.XPATH,
                        "//span[contains(text(),'نمایش شناسنامه')]").click()
            except:
                log(row['rahgiri'], 'پرونده ای موجود نیست')
                continue
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div"
            ).click()
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[3]/td[1]/a/div"
            ).click()
            driver.find_element(
                By.XPATH,
                '//*[@id="CPC_TextboxDisableTaxpayerDateFa"]').send_keys(
                    "1401/07/17")
            driver.find_element(
                By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]").click()
            driver.find_element(By.ID,
                                "CPC_DropDownCommentType_chosen").click()
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input'
            ).send_keys("سایر موارد")
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input'
            ).send_keys(df_to_excelsheet)
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[3]/td[2]/input'
            ).send_keys(
                "با توجه به نامه شماره 22213 و با درخواست اداره مربوطه غیرفعال گردید"
            )
            driver.find_element(
                By.XPATH, '//*[@id="CPC_CheckBoxDisableTaxpayer"]').click()
            driver.find_element(
                By.XPATH, '//*[@id="CPC_ButtonDisableTaxpayer"]').click()
            time.sleep(4)

            try:
                if (driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div"]'
                )):
                    log(
                        row['rahgiri'],
                        'به علت اتصال دستگاه کارتخوان امکان غیر فعالسازی نیست')
            except:
                log(row['rahgiri'], 'success')
        except Exception as e:
            log(row["rahgiri"], 'failure')
            print(e)
            time.sleep(1)
            continue

        time.sleep(1)

    driver.close()


def set_imp_corps(driver, df, save_path):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(0.5)

    # for index, row in df.loc[df['is_set'].isna()].iterrows():
    for index, row in df.loc[df['is_set'] == 'no'].iterrows():
        print(index)
        try:

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.ID,
                    "TextboxPublicSearch"
                ))).clear()

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.ID,
                    "TextboxPublicSearch"
                ))).send_keys(str(row['کدرهگيري ثبت نام']))

            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((
                    By.ID,
                    "publicSearchLink"
                ))).click()

            time.sleep(0.5)

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[8]/a[1]/span"
                ))).click()

            try:

                title = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span"
                    ))).text

                if title == "پرونده مهم":
                    df.loc[index, 'is_set'] = 'yes'
                    remove_excel_files(files=[save_path], postfix=['xlsx'])
                    df.to_excel(save_path, index=False)
                continue

            except:

                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div"
                    ))).click()

                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[2]/td/a/div"
                    ))).click()

                time.sleep(0.4)

                alert = driver.switch_to.alert
                alert.accept()
                time.sleep(3)

                try:

                    title = WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span"
                        ))).text

                    if title == "پرونده مهم":
                        df.loc[index, 'is_set'] = 'yes'

                    remove_excel_files(files=[save_path], postfix=['xlsx'])

                    df.to_excel(save_path, index=False)

                except Exception as e:
                    print(e)
                    df.loc[index, 'is_set'] = 'no'

                    remove_excel_files(
                        files=[save_path], postfix=['xlsx'])
                    df.to_excel(save_path, index=False)

                print('f')

        except Exception as e:
            df.loc[index, 'is_set'] = 'no'
            remove_excel_files(
                files=[save_path], postfix=['xlsx'])
            df.to_excel(save_path, index=False)
            continue

    remove_excel_files(
        files=[save_path], postfix=['xlsx'])

    df.to_excel(save_path, index=False)

    driver.close()


def set_user_permissions(driver, df):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(0.5)
    driver.find_element(
        By.XPATH,
        "/html/body/form/table/tbody/tr[2]/td[1]/div[16]"
    ).click()

    driver.find_element(
        By.XPATH,
        "/html/body/form/table/tbody/tr[2]/td[1]/div[17]/a[2]/div"
    ).click()

    time.sleep(1)
    for index, row in df.iterrows():
        print(index)
        try:
            driver.find_element(
                By.ID,
                "CPC_TextboxAdminUsernameNationalID"
            ).clear()

            driver.find_element(
                By.ID,
                "CPC_TextboxAdminUsernameNationalID"
            ).send_keys(str(row["rahgiri"]))

            driver.find_element(
                By.ID,
                'CPC_ButtonSearchuser').click()

            time.sleep(5)

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]/td/span/table/tbody/tr[3]/td[3]/a"
            ).click()

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[1]/tbody/tr/td[2]/a/div"
            ).click()

            lst_mojavez = driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[2]/tbody/tr/td/div/div[2]/table/tbody/tr/td[1]/table/tbody"
            )

            lst_all = lst_mojavez.find_elements(
                By.TAG_NAME,
                'td'
            )

            gardesh_poz = False
            sipa = False
            for item in lst_all:
                if item.text == '119':
                    gardesh_poz = True
                if item.text == '100101':
                    sipa = True

            if gardesh_poz & sipa:
                print('already granted')
                driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div"
                ).click()
                time.sleep(1)
                continue

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[2]/tbody/tr/td/div/div[2]/a[1]/div"
            ).click()

            time.sleep(6)

            try:
                if not gardesh_poz:
                    driver.find_element(
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[1]/table[2]/tbody/tr[1]/td[2]/table/tbody/tr[20]/td[1]/span/img"
                    ).click()

                    time.sleep(3)

                if not sipa:

                    driver.find_element(
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[1]/table[2]/tbody/tr[123]/td[2]/table/tbody/tr/td[1]/span/img"
                    ).click()

                    time.sleep(3)
            except Exception as e:
                print(e)
                print('not found')
                continue

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[1]/td/div/a/div"
            ).click()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div"
                )))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div").click()

        except Exception as e:
            print(e)
            continue

    driver.close()


def set_hoze(driver, df):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(1)
    for index, row in df.iterrows():
        try:
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[1]/input"
            ).send_keys(str(row["rahgiri"]))

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span"
                )))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span"
            ).click()

            if driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span"
            ).text == "غيرفعال":
                log(row["rahgiri"], 'قبلا غیر فعال شده است')
                # continue

            time.sleep(1)
            try:
                WebDriverWait(driver, 28).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "//span[contains(text(),'نمایش شناسنامه')]"
                    )))
                driver.find_element(
                    By.XPATH,
                    "//span[contains(text(),'نمایش شناسنامه')]").click()
            except:
                log(row['rahgiri'], 'پرونده ای موجود نیست')
                continue

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[5]/a/div"
                )))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[5]/a/div"
            ).click()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[5]/a[2]/div"
                )))

            sel_text = driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[5]/a[2]/div"
            ).text

            if sel_text == "تعیین مشمولیت ارزش افزوده":

                driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[5]/a[1]/div"
                ).click()

            else:
                driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[5]/a[2]/div"
                ).click()

            edare = driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[4]/td[2]/div/a/span"
            ).text

            edare = extract_nums(edare)

            if int(edare[0]) == 1607:
                continue

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[4]/td[2]/div/a/div/b"
            ).click()

            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[4]/td[2]/div/div/div/input').send_keys(
                    "1607")
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[4]/td[2]/div/div/div/input').send_keys(Keys.RETURN)
            driver.find_element(
                By.ID, "CPC_TextboxAddSanimOfficeIDUnitID").clear()
            driver.find_element(
                By.ID, "CPC_TextboxAddSanimOfficeIDUnitID").send_keys('160733')
            driver.find_element(
                By.ID, "CPC_TextboxAddSanimOfficeIDClass").clear()
            driver.find_element(
                By.ID, "CPC_TextboxAddSanimOfficeIDClass").send_keys('1')
            driver.find_element(By.ID,
                                "CPC_ButtonAddSanimOfficeID").click()
        except Exception as e:
            log(row["rahgiri"], 'failure')
            print(e)
            time.sleep(1)
            continue

        time.sleep(1)

    driver.close()


@wrap_it_with_params(10, 10, True, False, False, False)
def scrape_it(path, driver_type, headless=False, data_gathering=False, pred_captcha=False, driver=None, info={}):
    with init_driver(
            pathsave=path, driver_type=driver_type, headless=headless, driver=driver, info=info) as driver:
        path = path
        driver, info = login_codeghtesadi(
            driver=driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=info)

    return driver, info


def save_in_db(path, year):
    with open('saved_model_ngram1_2', 'rb') as f:
        model = pickle.load(f)
    dirs = []
    address_dict = get_soratmoamelat_mapping_address()
    dirs.extend([it.path for it in os.scandir(path) if it.is_dir()])
    final_dirs = dirs
    for item in dirs:
        final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])
    for item in final_dirs:
        tbl_name = item.split('\\')[-1]
        files = list_files(item, 'xlsx')
        notfirst = False
        if len(files) > 0:
            for file in files:
                df = pd.read_excel(file)
                cols = df.loc[0].values.tolist()
                df = df.iloc[1:, :]
                df = df.astype(str)
                df.columns = cols
                df['آخرین بروزرسانی'] = get_update_date()
                df['اداره'] = model.predict(
                    df[address_dict[tbl_name]].to_numpy())
                drop_into_db(table_name=tbl_name + '%s' % year,
                             columns=df.columns.tolist(),
                             values=df.values.tolist(), append_to_prev=notfirst)
                notfirst = True


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def close_modal_dadrasi(driver, info):

    if (WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[9]/div/div/div[3]/button'
            )))):

        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[9]/div/div/div[3]/button'
            ))).click()

    return driver, info


@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def click_on_search_dadrasi(driver, info):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/nav/div[2]/ul/li[5]/a/span'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_btnsearch_dadrasi(driver, info):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/form/div[11]/div/button'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_dadrasi_table(driver, info):
    info['table'] = WebDriverWait(driver, 4).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/table'
        )))
    return driver, info


@wrap_it_with_paramsv1(10, 10, True, False, False, True)
def wait_for_next_page_dadrasi(driver, info):
    text_changed = WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/div[2]/span[2]'
        ))).text

    driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div[2]/ul').\
        find_elements(By.TAG_NAME, 'li')[-1].click()

    while text_changed == WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[1]/div/div/div[2]/div[2]/span[2]'
            ))).text:
        print('waiting')
        time.sleep(2)
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def return_dadrasi_pages(driver, info):

    info['pages'] = int(extract_nums(WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/div[2]/ul/li[13]/a'
        ))).text)[0])

    return driver, info


def check_if_dadrasi_updated(pages, num_pages_updated=0):
    sql_query = """
    IF OBJECT_ID('tbldadrasi', 'U') IS NOT NULL
        BEGIN
            -- Table exists, so perform the SELECT query
            SELECT MAX([تاریخ بروزرسانی]) FROM tbldadrasi;
        END
        ELSE
        BEGIN
            -- Table does not exist, handle accordingly
            SELECT 0;
        END
    """
    df = connect_to_sql(sql_query, read_from_sql=True, return_df=True)

    if df.loc[0].item() == get_update_date():
        sql_query = "SELECT COUNT(*) FROM tbldadrasi"
        num_pages_updated = int(math.ceil(connect_to_sql(
            sql_query, read_from_sql=True, return_df=True).loc[0].item() / 50))

        if num_pages_updated == pages:
            return (True, 0)

    return (False, num_pages_updated)


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def navigate_to_next_dadrasi_page(driver=None, info={}, new_url='', timeout=30):
    driver.get(new_url)

    try:
        while (WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((
                    By.CLASS_NAME,
                    'busy-indicator'
                )))):
            print('waiting')
            time.sleep(1)
            timeout -= 1
            if timeout == 0:
                raise Exception
            else:
                continue
    except:
        if timeout == 0:
            raise Exception

    return driver, info


@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def wait_fordadrasi(driver=None, info={}):
    try:
        if (WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[1]/nav/div[2]/ul/li[5]/a/span'
                ))).is_displayed()):
            return driver, info
    except Exception as e:
        print('Waiting for the dadrasi application')
        raise Exception

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_link_dadrasi(driver=None, info={}):
    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div[3]/div[2]/div[2]/div/div[12]/div/div[2]/div/a/h6'
        ))).click()
    return driver, info


def wait_till_dadrasi_visible(driver=None, info={}):
    while True:
        driver, info = click_on_link_dadrasi(driver=driver, info=info)
        driver, info = wait_fordadrasi(driver=driver, info=info)
        if not info['success']:
            driver.back()
        else:
            return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_dadrasidata(driver=None, path=None, del_prev_files=True, info=None, *args, **kwargs):

    if del_prev_files:
        remove_excel_files(file_path=path, postfix=['.xls', '.html'])

    driver, info = wait_till_dadrasi_visible(driver=driver, info=info)

    driver, info = click_on_search_dadrasi(driver=driver, info=info)

    # driver, info = click_on_btnsearch_dadrasi(driver=driver, info=info)

    driver, info = get_dadrasi_table(driver=driver, info=info)

    driver, info = return_dadrasi_pages(driver=driver, info=info)

    is_updated, num_pages_updated = check_if_dadrasi_updated(info['pages'])

    if is_updated:
        return

    append_to_prev = True if num_pages_updated > 0 else False
    index = 0
    for index, value in enumerate(range(num_pages_updated + index, info['pages']+1)):
        if index == 0:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[1]/div/div/div[2]/div[2]/ul/li[14]/a/span'
                ))).click()
            driver, info = wait_for_next_page_dadrasi(driver=driver, info=info)
            continue

        new_url = re.sub(
            r"(%22\d+%22)", f"%22{value}%22", driver.current_url)

        driver, info = navigate_to_next_dadrasi_page(
            driver=driver, info=info, new_url=new_url)

        table = WebDriverWait(driver, 32).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[1]/div/div/div[2]/table'
            )))
        columns = table.find_elements(By.TAG_NAME, 'thead')[0].\
            find_elements(By.TAG_NAME, 'tr')[0].\
            find_elements(By.TAG_NAME, 'label')
        columns_names = list(filter(None, [name.text for name in columns]))
        # columns_names = [name.text for name in columns]

        table_rows = table.find_elements(By.TAG_NAME, 'tbody')[0].\
            find_elements(By.TAG_NAME, 'tr')
        table_tds = [item.find_elements(
            By.XPATH, 'td') for item in table_rows]

        lst = []
        for items in table_tds:
            lst_tmp = []
            for index, item in enumerate(items):
                if index not in [13]:
                    if ('...' in item.text and index != 10):
                        text = unquote(item.
                                       find_element(By.CLASS_NAME, 'text-ellipsise').
                                       get_attribute('data-text'))
                    else:
                        text = item.text
                    lst_tmp.append(text)

            lst.append(pd.Series(data=lst_tmp, index=columns_names))

        df = pd.concat(lst, axis=1).T
        df['تاریخ بروزرسانی'] = get_update_date()
        drop_into_db('tbldadrasi',
                     df.columns.tolist(),
                     df.values.tolist(),
                     append_to_prev=append_to_prev)
        append_to_prev = True

        driver, info = wait_for_next_page_dadrasi(driver=driver, info=info)

        print('...')

    print('...')


def get_eghtesadidata(driver=None, path=None, del_prev_files=True, *args, **kwargs):

    if del_prev_files:
        remove_excel_files(file_path=path, postfix=['.xls', '.html'])

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[1]/a[3]/div'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[1]/a[3]/div'
    ).click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/span'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/div/a[1]'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/div/a[1]'
    ).click()

    def if_downloaded():
        nonlocal is_done
        file_list = glob.glob(path + "/*" + '.xls')

        if len(file_list) == 0:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div/a'
                )))
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div/a'
            ).click()

            file_list = glob.glob(path + "/*" + '.xls.part')

            while len(file_list) != 0:

                time.sleep(5)
                print('waiting')
                file_list = glob.glob(path + "/*" + '.xls.part')

            time.sleep(5)

            file_list = glob.glob(path + "/*" + '.xls')
            if len(file_list) != 0:
                is_done = True

    is_done = False

    while not is_done:
        if_downloaded()


@log_the_func('none')
def scrape_arzeshafzoodeh_helper(path, del_prev_files=True,
                                 headless=False, driver_type='firefox',
                                 *args, **kwargs):
    if del_prev_files:
        remove_excel_files(file_path=path, postfix=[
                           '.xls', '.html'], field=kwargs['field'])
    driver = init_driver(pathsave=path,
                         driver_type=driver_type,
                         headless=headless,
                         field=kwargs['field'])

    driver = login_arzeshafzoodeh(driver, field=kwargs['field'])
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/a/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/a/span'
    ).click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/div/ul/li[16]/a/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/div/ul/li[16]/a/span'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2').click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3').click()

    def arzesh(i):
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_frm_year')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_frm_year'))
        sel.select_by_index(i)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_frm_period')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_frm_period'))
        sel.select_by_index(0)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_To_year')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_To_year'))
        sel.select_by_index(i)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_To_period')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_To_period'))
        sel.select_by_index(3)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_Button3')))
        time.sleep(10)
        driver.find_element(
            By.ID, 'ctl00_ContentPlaceHolder1_Button3').click()

    for i in range(0, 15):

        t1 = threading.Thread(target=arzesh, args=(i, ))
        t2 = threading.Thread(target=watch_over)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    file_list = glob.glob(path + "/*" + '.xls.part')
    # While the files are not completely downloaded just wait
    while len(file_list) != 0:

        time.sleep(1)
        file_list = glob.glob(path + "/*" + '.xls.part')

    time.sleep(3)
    driver.close()


def get_mergeddf_arzesh(path, return_df=True):
    dest = os.path.join(path, 'temp')
    file_list = glob.glob(path + "/*" + '.xls')
    if file_list:
        # dest = path
        rename_files(path,
                     dest=dest,
                     file_list=file_list,
                     years=lst_years_arzeshafzoodeSonati)
    df_arzesh = merge_multiple_html_files(path=dest,
                                          drop_into_sql=False,
                                          drop_to_excel=True,
                                          add_extraInfoToDf=True,
                                          return_df=True)

    if return_df:
        return df_arzesh


def insert_arzeshafzoodelSonati(df_arzesh=None):
    if df_arzesh is None:
        df_arzesh = pd.read_excel(
            r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\final_df.xlsx'
        )
    df_arzesh['شماره پرونده'] = df_arzesh['شماره پرونده'].astype(
        'int64')
    df_arzesh = df_arzesh.astype('str')
    drop_into_db('tblArzeshAfzoodeSonati',
                 df_arzesh.columns.tolist(),
                 df_arzesh.values.tolist(),
                 append_to_prev=False)


# Drop sabtenamArzeshAfzoode into sql
def insert_sabtenamArzeshAfzoodeh():
    df_sabtarzesh = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\ثبت نام ارزش افزوده.xlsx'
    )

    df_sabtarzesh = df_sabtarzesh[
        df_sabtarzesh['کدرهگیری'].notna()]
    df_sabtarzesh['کدرهگیری'] = df_sabtarzesh['کدرهگیری'].astype(
        'int64')
    df_sabtarzesh['شناسه'] = df_sabtarzesh['شناسه'].astype('int64')
    df_sabtarzesh = df_sabtarzesh.astype('str')
    drop_into_db('tblSabtenamArzeshAfzoode',
                 df_sabtarzesh.columns.tolist(),
                 df_sabtarzesh.values.tolist(),
                 append_to_prev=False)


def insert_codeEghtesadi():
    df_codeeghtesadi = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.xlsx'
    )
    df_codeeghtesadi['کد رهگیری'] = df_codeeghtesadi[
        'کد رهگیری'].astype('int64')
    df_codeeghtesadi = df_codeeghtesadi.astype('str')
    drop_into_db('tblSabtenamCodeEghtesadi',
                 df_codeeghtesadi.columns.tolist(),
                 df_codeeghtesadi.values.tolist(),
                 append_to_prev=False)


def insert_gashtPosti():

    df_gasht = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\گشت پستی استان.xlsx'
    )

    is_non_numeric = pd.to_numeric(df_gasht['گشت پستی'],
                                   errors='coerce').isnull()
    df_gasht = df_gasht[~is_non_numeric]
    index_gasht = df_gasht[df_gasht['گشت پستی']]
    df_gasht['گشت پستی'] = df_gasht['گشت پستی'].astype('int64')
    df_gasht['کد اداره امور مالیاتی'] = df_gasht[
        'کد اداره امور مالیاتی'].astype('int64')
    df_gasht = df_gasht.astype('str')
    drop_into_db('tblGashtPosti',
                 df_gasht.columns.tolist(),
                 df_gasht.values.tolist(),
                 append_to_prev=False)


def save_process(driver, path):
    global DOWNLOADED_FILES

    save = driver.find_element(By.ID, 'StiWebViewer1_SaveLabel')

    if (save.is_displayed()):
        actions = ActionChains(driver)
        actions.move_to_element(save).perform()
        hidden_submenu = driver.find_element(
            By.XPATH, '/html/body/form/div[3]/span/div[1]/div/table/tbody/tr/td[2]/table/tbody/tr/td[3]/div/div[2]/div/table/tbody/tr/td/table[12]/tbody/tr/td[5]')
        actions.move_to_element(hidden_submenu).perform()
        hidden_submenu.click()

        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.ID, "StiWebViewer1_StiWebViewer1ExportDataOnly")))
        element.click()

        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.ID, "StiWebViewer1_StiWebViewer1ExportObjectFormatting")))
        element.click()
        time.sleep(3)
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.XPATH, "/html/body/form/div[3]/span/table[1]/tbody/tr/td/table/tbody/tr[2]/td[2]/table/tbody/tr[6]/td/table/tbody/tr/td[1]/table/tbody/tr/td[2]")))
        time.sleep(1)
        element.click()


def scrape_1000_helper(driver):
    driver.find_element(
        By.XPATH,
        '/html/body/form/header/div[2]/div/ul/li[2]/span/span').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="t_MenuNav_1_5i"]').click()
    time.sleep(1)
    driver.find_element(
        By.XPATH, '//*[@id="t_MenuNav_1_5_2i"]').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((By.ID, 'select2-P377_NUMBER-container')))
    driver.find_element(
        By.ID, 'select2-P377_NUMBER-container').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
    driver.find_element(
        By.XPATH, '/html/body/span/span/span[1]/input').send_keys('1000')
    time.sleep(1)
    driver.find_element(
        By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)

    time.sleep(1)
    for year in ['1396', '1397', '1398', '1399']:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.ID, 'select2-P377_TAX_YEAR-container')))
        driver.find_element(
            By.ID, 'select2-P377_TAX_YEAR-container').click()
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
        driver.find_element(
            By.XPATH, '/html/body/span/span/span[1]/input').send_keys(year)
        driver.find_element(
            By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)

        for item in ['مالیات بر درآمد شرکت ها', 'مالیات بر درآمد مشاغل']:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, 'select2-P377_TAX_TYPE-container')))
            driver.find_element(
                By.ID, 'select2-P377_TAX_TYPE-container').click()
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
            driver.find_element(
                By.XPATH, '/html/body/span/span/span[1]/input').send_keys(item)
            driver.find_element(
                By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)
            driver.find_element(
                By.ID, 'B151566902969448513').click()
            time.sleep(0.4)
            try:
                element = driver.find_element(
                    By.CLASS_NAME, 'u-Processing-spinner')
                while (element.is_displayed() == True):
                    print("waiting for the login to be completed")

            except Exception as e:
                time.sleep(0.5)
                driver.find_element(
                    By.ID, 'B480700311704515012').click()
                time.sleep(0.4)
                print(e)
                try:
                    element = driver.find_element(
                        By.CLASS_NAME, 'u-Processing-spinner')
                    while (element.is_displayed() == True):
                        print("waiting for the download to complete")

                except Exception as e:

                    time.sleep(3)

    time.sleep(10)
    driver.close()


def create_1000parvande_report(path, file_name, df, save_dir, *args, **kwargs):

    file_name = os.path.join(path, file_name)

    def is_ghatee(num):
        if len(num) > 6:
            return 'yes'
        else:
            return 'no'

    df['ghatee'] = df['شماره برگ قطعی'].apply(
        lambda x: is_ghatee(x))

    # df.to_excel(save_dir)
    cols = ['سال', 'منبع', 'اداره', 'درصد قطعی شده',
            'تعداد', 'تعداد قطعی شده']
    df_g = df.groupby(
        ['سال عملکرد', 'منبع مالیاتی', 'نام اداره فعلی'])
    lst = []

    df_to_excelsheet(file_name, df_g, index='', names=[
        'اداره ی امور مالیاتی', 'مالیات بر درآمد'])

    for key, item in df_g:
        ls = []
        count = item['ghatee'].count()
        yes_count = item['ghatee'][item['ghatee'] == 'yes'].count()
        ls.append(key[0])
        ls.append(key[1])
        ls.append(key[2])
        ls.append(yes_count/count)
        ls.append(count)
        ls.append(yes_count)
        lst.append(ls)

    new_df = pd.DataFrame(lst, columns=cols)

    p_df_1 = pd.pivot_table(new_df, values=['درصد قطعی شده', 'تعداد قطعی شده', 'تعداد'], aggfunc=sum,
                            index='اداره', columns=['سال', 'منبع'],
                            fill_value=0, margins=True, margins_name='جمع کلی')

    p_df_1.to_excel(
        os.path.join(path, 'final_agg_pv.xlsx'))


def get_modi_info(driver=None, path=None, df=None, saving_dir=None, *args, **kwargs):

    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
    ).click()

    lst_names = []

    for index, row in df.iterrows():
        try:
            print(f'Iteration no {index}')
            print(
                '***************************************************************************************')
            row = str(row.values[0])
            if len(row) == 8:
                row = '00' + row
            elif len(row) == 9:
                row = '0' + row
            modi = Modi()
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.ID,
                    'TextboxPublicSearch'
                )))
            driver.find_element(
                By.ID,
                'TextboxPublicSearch'
            ).send_keys(row)

            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span'
                )))
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span'
            ).click()

            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[2]/td[2]/span/table[1]/tbody/tr[2]/td[8]/a/span'
                ))).click()

            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr/td[3]/table/tbody/tr[11]/td/a/div'
                ))).click()

            try:
                modi_pazirande = Modi_pazerande()
                modi_pazirande.melli = row
                time.sleep(1)
                if (WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table/tbody'
                        )))):

                    elm = driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table/tbody'
                    )

                    rws = elm.find_elements(By.TAG_NAME, 'tr')[1:-1]

                    for item in rws:
                        modi_pazirande = Modi_pazerande()
                        tds = item.find_elements(By.TAG_NAME, 'td')
                        modi_pazirande.melli = row
                        modi_pazirande.hoviyati = tds[0].text
                        modi_pazirande.code = tds[3].text
                        modi_pazirande.shomarepayane = tds[4].text
                        modi_pazirande.vaziat = tds[5].text
                        modi_pazirande.vaziatelsagh = tds[6].text
                        modi_pazirande.money1400 = tds[9].text
                        modi_pazirande.money1401 = tds[10].text
                        lst_names.append([modi_pazirande.melli,
                                          modi_pazirande.hoviyati,
                                          modi_pazirande.code,
                                          modi_pazirande.shomarepayane,
                                          modi_pazirande.vaziat,
                                          modi_pazirande.vaziatelsagh,
                                          modi_pazirande.money1400,
                                          modi_pazirande.money1401])

                        if (len(lst_names) % 10 == 0):

                            columns = ['شناسه هویتی پذیرنده', 'کد پذیرنده فروشگاهی', 'شماره پایانه',
                                       'وضعیت پذیرنده', 'وضعیت پذیرنده', 'وضعیت الصاق',
                                       'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1400',
                                       'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1401'
                                       ]

                            final_df = pd.DataFrame(
                                lst_names[-10:], columns=columns)

                            file_name = os.path.join(
                                saving_dir, 'last10of%s.xlsx' % len(lst_names))

                            final_df.to_excel(file_name)

            except:
                lst_names.append([row, 'None',
                                  'None', 'None', 'None', 'None', 'None', 'None'])

                if (len(lst_names) % 10 == 0):

                    columns = ['شناسه هویتی پذیرنده', 'کد پذیرنده فروشگاهی', 'شماره پایانه',
                               'وضعیت پذیرنده', 'وضعیت پذیرنده', 'وضعیت الصاق',
                               'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1400',
                               'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1401'
                               ]

                    final_df = pd.DataFrame(lst_names[-10:], columns=columns)

                    file_name = os.path.join(
                        saving_dir, 'last10of%s.xlsx' % len(lst_names))

                    final_df.to_excel(file_name)
        except Exception as e:
            print(e)
            continue
        # modi_pazirande.hoviyati = driver.find_element(
        #     By.XPATH,
        #     '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[1]'
        # ).text

        # time.sleep(0.5)
        # try:

        #     modi.melli = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[1]'
        #     ).text

        #     modi.sex = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[2]'
        #     ).text

        #     modi.name = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[3]'
        #     ).text

        #     modi.father_name = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[4]'
        #     ).text

        #     modi.dob = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[5]'
        #     ).text

        #     modi.id_num = driver.find_element(
        #         By.XPATH,
        #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[6]'
        #     ).text

        #     lst_names.append([modi.melli, modi.sex, modi.name,
        #                       modi.father_name, modi.dob, modi.id_num])

        # except Exception as e:
        #     lst_names.append([row, 'None',
        #                      'None', 'None', 'None', 'None'])
        #     continue

        # if (len(lst_names) % 10 == 0):

        #     columns = ['کد ملی', 'جنسیت', 'نام',
        #                'نام پدر', 'تاریخ تولد', 'شماره شناسنامه']

        #     final_df = pd.DataFrame(lst_names[-10:], columns=columns)

        #     file_name = os.path.join(
        #         saving_dir, 'last10of%s.xlsx' % len(lst_names))

        #     final_df.to_excel(file_name)


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 10, True, False, False, True)
def check_if_shenase_exists(driver, info):
    WebDriverWait(driver, 4).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/\
                td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[2]/div')))
    return driver, info


def check_if_value_inserted(driver, item):
    if (WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input'))).
            get_attribute('value') == str(int(item[0]))):
        return driver
    else:
        raise Exception


# @wrap_a_wrapper
@wrap_it_with_paramsv1(10, 10, True, False, False, True)
def insert_value(driver, item, info):
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/\
                 td/table/tbody/tr[2]/td[2]/div/input'))).clear()
    time.sleep(2)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/\
                 td/table/tbody/tr[2]/td[2]/div/input'))).send_keys(str(item[0]))

    driver = check_if_value_inserted(driver, item)

    return driver, info


@wrap_it_with_paramsv1(2, 10, True, False, False, False)
def get_info_shenase_dadrasi(driver, info):
    info['shenase_dadrasi_num'] = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/\
                td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[22]/div'))).text
    info['vaziat'] = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/\
                tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[6]/div'))).text
    info['assigned'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]\
            /td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div').text

    return driver, info


@wrap_it_with_paramsv1(8, 10, True, False, False, True)
def click_on_search_btn(driver, info):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/\
                ul/li[4]/div/div[1]/div[2]/input'))).click()

    while (True):
        try:
            if (WebDriverWait(driver, 1).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[6]'))).is_displayed()):

                time.sleep(1)
                continue
        except:
            time.sleep(1)
            break

    return driver, info


def check_success(info):
    for k, v in info.items():
        if isinstance(v, bool):
            if not v:
                info['success'] = False
                return info
    return info

# @wrap_a_wrapper


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 10, True, False, False, True)
def clear_and_send_keys(driver, item, info, role='manager_phase1'):
    if not info['success']:
        raise Exception

    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

    if not info['success']:
        raise Exception

    driver, info = check_if_shenase_exists(driver=driver, info=info)

    if not info['success']:
        raise Exception

    # info['success'] = False if len(shenase_dadrasi_num) == 1 else True

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def select_row(driver, info):
    WebDriverWait(driver, 7).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/\
                tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div'))).click()

    time.sleep(2)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(12, 12, True, False, False, False)
def assign_btn(driver, role='manager_phase2', shenase_dadrasi='no',
               info={'success': True, 'keep_alive': True}):

    if (role == 'employee' or shenase_dadrasi == 'yes'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/\
                    ul/li[10]/div/div[1]/div[2]/input'))).click()
    else:
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/\
                     div[2]/table/tbody/tr/td[3]/div'))).click()

    time.sleep(1)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(12, 30, True, False, False, True)
def get_info(driver, role, info):

    info['data'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]\
            /td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div').text
    info['vaziat'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody\
            /tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[6]/div').text
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 60, False, False, False)
def save_excel(index, df, file_name, backup_file, done='yes', success='yes',
               driver=None, second_phase='no', role='manager_phase2', save_every_main=2,
               save_every_back=10, shenase_dadrasi='no', shenase_dadrasi_num=None, info={}):
    if role == 'manager_phase2':
        df[0].loc[index, 'is_done'] = done
        df[0].loc[index, 'success'] = success
        df[0].loc[index, 'assigned_to'] = str(
            df[1]['employee'].head(1).item())
        df[0].loc[index, 'shenase_dadrasi'] = shenase_dadrasi
        df[0].loc[index, 'shenase_dadrasi_no'] = shenase_dadrasi_num
    else:
        df[0].loc[index, 'second_phase'] = second_phase

    remove_excel_files(files=[file_name],
                       postfix='xlsx')
    df[0].to_excel(
        file_name, index=False)

    # if index % save_every_main == 0:

    #     remove_excel_files(files=[file_name],
    #                        postfix='xlsx')
    #     df[0].to_excel(
    #         file_name, index=False)

    # if index % save_every_back == 0:
    #     remove_excel_files(files=[file_name],
    #                        postfix='xlsx')
    #     df[0].to_excel(
    #         backup_file, index=False)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(11, 3, True, False, False, False)
def go_to_next_frame(driver, role='manager_phase2', shenase_dadrasi='no', info={}):
    if (role == 'manager_phase2' and shenase_dadrasi == 'no'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[6]/div/div[1]/div[2]/input'))).click()
    time.sleep(4)
    driver.switch_to.default_content()
    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
    driver.switch_to.frame(frame)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, False, True)
def handle_error(driver, info={}):

    while True:
        info['fraud'] = 'nofraud'
        try:
            driver.switch_to.default_content()

            if (WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
                driver.find_element(
                    By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
                time.sleep(2)
                driver.find_element(
                    By.XPATH, '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]').click()

                time.sleep(1)

                info['fraud'] = 'isfraud'

                frame = driver.find_element(
                    By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
                driver.switch_to.frame(frame)
                return driver, info
            # else:
            #     fraud = 'isfraud'
            #     frame = driver.find_element(
            #         By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
            #     driver.switch_to.frame(frame)
            #     return driver, fraud

        except Exception as e:
            try:
                frame = driver.find_element(
                    By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
                driver.switch_to.frame(frame)
                WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]')))
                info['fraud'] = 'isfraud'
                return driver, info
            except:
                return driver, info
        except:

            return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, True, False)
def if_apply_new_assignment(driver, role='manager_phase2', info={}):
    if role == 'manager_phase2':
        try:
            try:
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                            '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/\
                                tr[2]/td[4]/div/input[2]')))
            except:
                return driver, info
            raise Exception
        except:
            raise Exception

    else:
        try:
            try:
                j = 0
                while (WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[6]/td/table/tbody/tr[2]/td/table/\
                             tbody/tr[10]/td[2]/div/input')))):
                    time.sleep(1)
                    j += 1
                    if j < 10:
                        continue
                    break
            except:
                return driver, info
            raise Exception
        except Exception:
            raise Exception

    # if text == 'مورد اعتراض/شکایت':
    #     raise Exception


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, True, False)
def apply_new_assignment(driver, role='manager_phase2', info={}):
    if role == 'manager_phase2':
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[2]/div/div[1]/div[2]/input'
    else:
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[4]/div/input'
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             xpath))).click()
    time.sleep(2)
    driver, info = handle_error(driver, info)
    if info['fraud'] == 'isfraud':
        # return driver, fraud
        raise Exception
    driver, info = if_apply_new_assignment(driver=driver, role=role, info=info)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, False, False)
def click_submit_btn(driver, info):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[2]/ul/li[4]/div/div[1]/div[2]/input'))).click()

    i = 0
    while (len(driver.window_handles) == 1):
        time.sleep(1)
        i += 1
        print('waiting')
        if i > 60:
            raise Exception
        continue
    time.sleep(2)
    # Close the child window
    driver.switch_to.window(driver.window_handles[1])
    driver.close()
    # Switch to the main window
    driver.switch_to.window(driver.window_handles[0])
    driver.switch_to.default_content()
    try:
        if (WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
            message = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     '/html/body/div[8]/p'))).text
            driver.find_element(
                By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
            # Close
            WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]'))).click()
            info['success'] = False
            info['message'] = message
            frame = driver.find_element(
                By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
            driver.switch_to.frame(frame)
            return driver, info

        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

    except:
        frame = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
        driver.switch_to.frame(frame)

        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/\
                     td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[22]/div'))).text
        driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
        return driver, info

    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, False, False)
def insert_new_assignment(driver,
                          df,
                          index,
                          role='manager_phase2',
                          item=None,
                          shenase_dadrasi='no',
                          file_name=None,
                          backup_file=None,
                          save_every_main=None,
                          save_every_back=None,
                          info={}):

    if (role == 'manager_phase2' and shenase_dadrasi == 'no'):
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]'
        input_str = str(df[1]['employee'].head(1).item())

    elif (role == 'manager_phase2' and shenase_dadrasi == 'yes'):
        driver, info = click_submit_btn(driver=driver, info=info)

        # driver, info = check_health(
        #     driver=driver,
        #     index=index,
        #     df=df,
        #     file_name=file_name,
        #     backup_file=backup_file,
        #     serious=False,
        #     role=role,
        #     init=False,
        #     save_every_main=save_every_main,
        #     save_every_back=save_every_back,
        #     info=info)

        if not info['success']:
            raise Exception

        return driver, info

    elif (role == 'employee'):
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[6]/td/table/tbody/tr[2]/td/\
            table/tbody/tr[10]/td[2]/div/input'
        input_str = str(item['daramad_tashkhis'])

    time.sleep(2)

    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                xpath))).click()
    time.sleep(3)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                xpath))).send_keys(input_str)

    time.sleep(2)
    driver, info = apply_new_assignment(driver, role=role, info=info)

    if info['fraud'] == 'isfraud':
        raise Exception

    # if isinstance(driver, tuple):
    #     driver, success = driver
    #     if isinstance(success, str):
    #         if success == 'isfraud':
    #             raise Exception
    #         return driver, info
    #     raise Exception
    time.sleep(1)

    return driver, info


def check_health(driver,
                 index=None,
                 df=None,
                 file_name=None,
                 backup_file=None,
                 serious=True,
                 role='manager_phase2',
                 init=False,
                 save_every_main=2,
                 save_every_back=10,
                 info={}):

    if (not info['success'] or len(info['shenase_dadrasi_num']) > 1
            or (role == "manager_phase2" and info['vaziat'] == 'ثبت شده توسط خدمات مودیان')):
        driver, info = save_excel(index, df, file_name, backup_file,
                                  done='yes', success='yes', second_phase='yes',
                                  role=role, save_every_main=save_every_main,
                                  save_every_back=save_every_back,
                                  shenase_dadrasi='yes', shenase_dadrasi_num=info['shenase_dadrasi_num'],
                                  driver=driver, info=info)
        info['success'] = False
        return driver, info

    if 'shenase_dadrasi_num' in info:
        if len(info['shenase_dadrasi_num']) > 1:
            driver, info = save_excel(index, df, file_name, backup_file,
                                      done='yes', success='yes', second_phase='yes',
                                      role=role, save_every_main=save_every_main,
                                      save_every_back=save_every_back,
                                      shenase_dadrasi='yes', shenase_dadrasi_num=info['shenase_dadrasi_num'],
                                      driver=driver, info=info)
            info['success'] = False
            return driver, info

    if info['success'] is not None:
        if not info['success']:
            if init:
                return driver, info
            driver, info = save_excel(index=index, df=df, file_name=file_name, backup_file=backup_file,
                                      done='yes', success='no', second_phase='no', role=role, save_every_main=save_every_main,
                                      save_every_back=save_every_back, driver=driver, shenase_dadrasi_num=info['message'], info=info)
            if serious:
                driver = login_iris(driver, creds={'username': df[1][role].head(1).item(),
                                                   'pass': df[1]['pass'].head(1).item()})
                driver = find_obj_and_click(driver)

            return driver, info

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(9, 10, True, False, True, False)
def find_obj_and_click(driver, info):
    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.ID,
                'OBJ')))
    driver.find_element(
        By.ID,
        'OBJ').click()

    time.sleep(3)

    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.LINK_TEXT,
                'موارد اعتراض/ شکایت'))).click()

    time.sleep(3)

    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
    driver.switch_to.frame(frame)

    WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                '//*[@id="RequestNo"]'))).click()
    time.sleep(1)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 70, True, False, False, False)
def scrape_iris_helper(stop_threads,
                       index,
                       item,
                       driver,
                       df,
                       file_name,
                       backup_file,
                       role,
                       save_every_main,
                       save_every_back,
                       shenase_dadrasi='no',
                       info={},
                       *args,
                       **kwargs):
    # while (True):
    # try:

    time.sleep(1)

    try:

        driver, info = insert_value(driver=driver, item=item, info=info)
        time.sleep(1)
        driver, info = click_on_search_btn(driver=driver, info=info)
        driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
    except:
        driver, info = check_health(
            driver=driver,
            index=index,
            df=df,
            file_name=file_name,
            backup_file=backup_file,
            serious=False,
            role=role,
            init=False,
            save_every_main=save_every_main,
            save_every_back=save_every_back,
            info=info)
        return driver, info

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        file_name=file_name,
        backup_file=backup_file,
        serious=False,
        role=role,
        init=False,
        save_every_main=save_every_main,
        save_every_back=save_every_back,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(2)

    driver, info = select_row(driver=driver, info=info)

    if not info['success']:
        return driver, info

    if role == 'manager_phase2':
        # if condition true then continue with the next item

        if (((info['assigned'] == str(df[1]['employee'].head(1).item()) and
                info['vaziat'] == 'ثبت شده توسط خدمات مودیان') or
                len(info['shenase_dadrasi_num']) > 3)):
            driver, info = save_excel(index=index, df=df,
                                      file_name=file_name, backup_file=backup_file,
                                      done='yes', success='yes',
                                      driver=driver, second_phase='yes',
                                      role=role, save_every_main=save_every_main,
                                      save_every_back=save_every_back,
                                      shenase_dadrasi='yes',
                                      shenase_dadrasi_num=info['shenase_dadrasi_num'],
                                      info=info)
            return driver, info
        if info['vaziat'] == 'ثبت شده':
            shenase_dadrasi = 'yes'
    elif role == 'employee':

        if info['vaziat'] == 'ثبت شده':

            driver, info = save_excel(index, df, file_name, backup_file,
                                      'yes', 'yes', driver, 'yes', role, save_every_main,
                                      save_every_back, shenase_dadrasi=shenase_dadrasi)
            return driver, info

        if info['vaziat'] != 'ثبت شده توسط خدمات مودیان':

            driver, info = save_excel(index, df, file_name, backup_file,
                                      'no', 'yes', driver, 'no', role, save_every_main,
                                      save_every_back, shenase_dadrasi=shenase_dadrasi)
            return driver, info

    # if condition false, then assign taskto the new user
    driver, info = assign_btn(driver=driver, role=role,
                              shenase_dadrasi=shenase_dadrasi, info=info)

    time.sleep(1)
    try:
        driver.switch_to.default_content()
        if (WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="flexPopupCancelMsgBtn"]')))):
            driver.find_element(
                By.XPATH, '//*[@id="flexPopupCancelMsgBtn"]').click()
        frame = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

        driver.switch_to.frame(frame)
        info['success'] = False
    except:
        ...
    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        file_name=file_name,
        backup_file=backup_file,
        serious=True,
        role=role,
        init=False,
        save_every_main=save_every_main,
        save_every_back=save_every_back,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(1)

    driver, info = go_to_next_frame(driver=driver, role=role,
                                    shenase_dadrasi=shenase_dadrasi, info=info)

    driver, success = check_health(
        driver=driver,
        index=index,
        df=df,
        file_name=file_name,
        backup_file=backup_file,
        serious=True,
        role=role,
        init=False,
        save_every_main=save_every_main,
        save_every_back=save_every_back,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(2)

    driver, info = insert_new_assignment(
        driver=driver, df=df,
        index=index, role=role, item=item,
        shenase_dadrasi=shenase_dadrasi,
        file_name=file_name, backup_file=backup_file,
        save_every_main=save_every_main,
        save_every_back=save_every_back,
        info=info)

    if len(info['shenase_dadrasi_num']) > 3:
        driver, info = save_excel(index=index, df=df, file_name=file_name, backup_file=backup_file,
                                  done='yes', success='yes', driver=driver, second_phase='yes', role=role, save_every_main=save_every_main,
                                  save_every_back=save_every_back,
                                  shenase_dadrasi=shenase_dadrasi,
                                  shenase_dadrasi_num=info['shenase_dadrasi_num'],
                                  info=info)
        return driver, info

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        file_name=file_name,
        backup_file=backup_file,
        serious=True,
        role=role,
        init=False,
        save_every_main=save_every_main,
        save_every_back=save_every_back,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(1)
    shenase_dadrasi_num = None
    time.sleep(2)
    if (role == 'employee'):
        driver.switch_to.default_content()
        frame = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

        driver.switch_to.frame(frame)

    if (role == 'manager_phase2' and shenase_dadrasi == 'yes'):
        dadrasi_xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td' + \
            '/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[22]/div'

        shenase_dadrasi_num = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    dadrasi_xpath))).text

    driver = save_excel(index=index, df=df, file_name=file_name, backup_file=backup_file,
                        done='yes', success='yes', driver=driver, second_phase='yes', role=role, save_every_main=save_every_main,
                        save_every_back=save_every_back,
                        shenase_dadrasi=shenase_dadrasi, shenase_dadrasi_num=shenase_dadrasi_num, info=info)

    return driver, info
# except Exception as e:
#     if isinstance(driver, tuple):
#         driver, success = driver

#     driver = save_excel(index, df, file_name,
#                         backup_file, 'yes', 'no', driver=driver)
#     driver = login_iris(driver, creds={'username': df[1]['user'].head(1).item(),
#                                        'pass': df[1]['pass'].head(1).item()})
#     time.sleep(2)
#     driver = find_obj_and_click(driver)


def old_sanim_way():
    with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as driver:
        self.driver = driver
        global excel_file_names

        self.driver, self.info = login_sanim(
            driver=self.driver, info=self.info)
        if self.report_type == 'ezhar':
            download_button = download_button_ezhar
        else:
            download_button = download_button_rest

            # انتخاب منوی گزارشات اصلی
        self.driver, self.info = get_main_menu(
            driver=self.driver, info=self.info)

        td_number = get_td_number(report_type=self.report_type)

        if (self.report_type == '1000_parvande'):

            self.driver, self.info = download_1000_parvandeh(self.driver, self.report_type,
                                                             self.year, self.path, self.info)

        else:
            self.driver, self.info = select_year(
                driver=self.driver, info=self.info, year=self.year)

            self.driver, self.info = select_column(
                driver=self.driver, info=self.info, td_number=td_number)
            # دریافت اظهارنامه ها و تشخیص های صادر شده
            exists_in_first_list = first_list.count(td_number)

            if (exists_in_first_list):

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[0]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    self.driver, self.info = list_details(driver=self.driver,
                                                          info=self.info,
                                                          report_type=self.report_type,
                                                          manba='hoghoghi')

                    self.driver, self.info = select_btn_type(
                        driver=self.driver, info=self.info, report_type=self.report_type)

                    print(
                        '*******************************************************************************************'
                    )
                    download_excel(
                        path=self.path,
                        report_type=self.report_type,
                        type_of_excel='Hoghoghi',
                        no_files_in_path=0,
                        excel_file=excel_file_names[0],
                        year=self.year,
                        table_name=self.table_name,
                        type_of=self.type_of)
                    self.driver.back()

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[1]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    if (self.report_type != 'ezhar'):

                        self.driver, self.info = list_details(driver=self.driver,
                                                              info=self.info,
                                                              report_type=self.report_type,
                                                              manba='haghighi')

                        self.driver, self.info = select_btn_type(
                            driver=self.driver, info=self.info, report_type=self.report_type)

                    else:
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, td_ezhar % 4))).click()

                    print(
                        '*******************************************************************************************'
                    )

                    download_excel(path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel='Haghighi',
                                   no_files_in_path=0,
                                   excel_file=excel_file_names[1],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)
                    self.driver.back()

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[2]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    if (self.report_type != 'ezhar'):
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, '/html/body/form/div[2]/div/div[2]/main/\
                                            div[2]/div/div/div/div/font/div\
                                            /div/div[2]/div[2]/div[5]/div[1]/div/div[2]\
                                            /table/tbody/tr[2]/td[8]/a'))).click()
                    else:
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, td_ezhar % 8))).click()

                    time.sleep(4)
                    WebDriverWait(self.driver, time_out_1).until(
                        EC.presence_of_element_located(
                            (By.XPATH, download_button))).click()

                    print(
                        '*******************************************************************************************'
                    )

                    download_excel(path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel='Arzesh Afzoode',
                                   no_files_in_path=0,
                                   excel_file=excel_file_names[2],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)
                    self.driver.back()

            # if there is only one report and no distinction between haghighi, hoghoghi and arzesh afzoode
            else:

                time.sleep(3)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, year_button_6))).click()
                time.sleep(1)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, year_button_4))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, switch_to_data))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, download_excel_btn_1))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, download_excel_btn_2))).click()
                self.type_of = 'download'
                download_excel(path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)

        # else:
        time.sleep(self.time_out)


@time_it(log=True, db={'db_name': 'testdbV2', 'tbl_name': 'tblLog', 'append_to_prev': True})
def download_excel(func, path=None, report_type=None, type_of_excel=None,
                   no_files_in_path=None, excel_file=None, table_name=None, year=None,
                   type_of=None, file_postfixes=['html', 'xlsx']):
    # i = 0

    # while len(glob.glob1(path, '%s' % excel_file)) == no_files_in_path:
    #     if i % 60 == 0:
    #         print('waiting %s seconds for the file to be downloaded' % i)
    #     i += 1
    #     time.sleep(1)

    t1 = threading.Thread(target=func)
    t2 = threading.Thread(
        target=watch_over, args=(path, 1200, 2))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    wait_for_download_to_finish(path, file_postfixes)
    print('****************%s done*******************************' % type_of_excel)

    # for prefix in file_postfixes:
    #     file_list = glob.glob(path + "/*" + prefix)

    # for item in file_list:
    #     os.rename(item, os.path.join(item.rsplit('\\', 1)[0],
    #                                  item.rsplit('\\', 1)[1].split('.')[0]+report_type+year+))
    # # return path + '\\' + excel_file


def scrape_sanim(self, *args, **kwargs):
    with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as self.driver:
        self.driver, self.info = login_sanim(
            driver=self.driver, info=self.info)
        self.driver, self.info = get_main_menu(
            driver=self.driver, info=self.info)
        self.driver, self.info = select_year(
            driver=self.driver, info=self.info, year=self.year)
        td_number = get_td_number(report_type=self.report_type)
        self.driver, self.info = select_column(
            driver=self.driver, info=self.info, td_number=td_number)
        if self.report_type not in ['badvi_darjarian_dadrasi',
                                    'badvi_takmil_shode',
                                    'tajdidnazer_darjarian_dadrasi',
                                    'tajdidnazar_takmil_shode']:
            links = get_report_links(report_type=self.report_type)
            for link in links:
                self.driver, self.info = click_on_down_btn_sanim(
                    driver=self.driver, info=self.info, link=link)

                download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)
                self.driver.back()

        else:
            self.driver, self.info = click_on_down_btn_excelsanimforheiat(
                driver=self.driver, info=self.info)
            download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info),
                           path=self.path,
                           report_type=self.report_type,
                           type_of_excel=self.report_type,
                           no_files_in_path=0,
                           excel_file=badvi_file_names[0],
                           year=self.year,
                           table_name=self.table_name,
                           type_of=self.type_of)
            self.driver.back()

    return self.driver, self.info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def select_from_dropdown(driver, info, xpath, dropitem='تاریخ تشکیل هیات'):

    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).click()
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).\
        send_keys(dropitem)
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).\
        send_keys(Keys.RETURN)

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def set_start_date(driver, info, xpath, date):
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).clear()
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).send_keys(date)
    time.sleep(2)
    if (WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).get_attribute("value") == date):

        return driver, info
    else:
        raise Exception


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_sodor_gharar_karshenasi(driver, info):
    # وارد کردن تاریخ شروع

    driver, info = set_start_date(
        driver=driver, info=info, xpath='P380_START_DATE', date='1390/01/01')

    # انتخاب تاریخ تشکیل هیات
    driver, info = select_from_dropdown(driver=driver, info=info, xpath='/html/body/\
        form/div[1]/div/div[2]\
        /main/div[2]/div\
        /div[1]/div/div/div/div[1]/div[1]/div/div[2]\
            /div/span/span[1]/span', dropitem='تاریخ تشکیل هیات')
    # انتخاب قرار اجرا شده و نشده
    driver, info = select_from_dropdown(driver=driver, info=info, xpath='/html/body/form\
        /div[1]/div/div[2]/\
        main/div[2]/div/div[1]/div/div\
        /div/div[2]/div[1]/div/div[2]/\
            div/span/span[1]/span', dropitem='قرار اجرا شده و نشده')
    # کلیک کلید جستجو
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, 'B200056751384075521'))).click()

    try:
        try:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div\
                        /div/div/div/div[2]/div[2]/div[5]/div/span'))).text == 'اطلاعاتی برای نمایش یافت نشد'):
                time.sleep(2)
                print('waiting')
        except:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                print('waiting')
    except:
        time.sleep(1)
        print('done')

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_amar_sodor_ray(driver, info):
    # وارد کردن تاریخ شروع

    driver, info = set_start_date(
        driver=driver, info=info, xpath='P482_START_DATE_RAY', date='1390/01/01')

    driver, info = set_start_date(
        driver=driver, info=info, xpath='P482_END_DATE_RAY',
        date=f'{get_update_date()[:4]}/{get_update_date()[4:6]}/{get_update_date()[6:8]}')

    # کلیک کلید جستجو
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, 'B1451081100278449861'))).click()

    try:
        try:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'icon-irr-no-results'))).is_displayed()):
                time.sleep(2)
                print('waiting')
        except:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                print('waiting')
    except:
        time.sleep(1)
        print('done')

    return driver, info

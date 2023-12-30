import os
import time
import math
import cx_Oracle
import numpy as np
import pandas as pd
from automation.helpers import connect_to_sql, get_update_date, time_it, georgian_to_persian, \
    is_date, is_int, drop_into_db, check_if_col_exists, log_the_func
from automation.helpersV2 import rename_duplicates
from automation.sql_queries import sql_delete, create_sql_table, insert_into, get_sql_allsanim, get_sql_allejra, \
    get_sql_sanimusers, get_sql_allhesabrasi, get_sql_alleterazat, \
    get_sql_allbakhshodegi, get_sql_allpayment, get_sql_allcodes, \
    get_sql_allhesabdari, get_sql_allanbare, get_sql_sanim_count, get_tax_types
import schedule
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from automation.constants import get_sql_con
from automation.vportal_reports.v_portal_reports import rpt_tashkhis_ghatee_238, get_important_modis

MULTI_THREADING = False
# Number of rows fetched every time connecting to oracle
NUM_ROWS = 500
TIME_TO_RUN = '08:24'

base_path = r'E:\automating_reports_V2\mapping'

excel_files = {
    'allsanim': 'allsanim.xlsx',
    'allusers': 'allusers.xlsx',
    'allhesabrasi': 'allhesabrasi.xlsx',
    'alleterazat': 'alleterazat.xlsx',
    'allbakhshodegi': 'allbakhshodegi.xlsx',
    'allhesabdari': 'allhesabdari.xlsx',
    'allejra': 'allejra.xlsx',
    'allanbare': 'allanbare.xlsx',
    'allpayment': 'allpayment.xlsx',
    'allcodes': 'allcodes.xlsx',
}

tbl_names = {
    'allsanim': 'V_PORTAL',
    'allusers': 'V_USERS',
    'allcodes': 's200_codes_22',
    'allhesabrasi': 'V_AUD35',
    'alleterazat': 'V_OBJ60',
    'allbakhshodegi': 'V_IMPUNITY',
    'allhesabdari': 'V_EHMOADI',
    'allejra': 'V_CASEEJRA',
    'allanbare': 'V_AUDPOOL',
    'allpayment': 'V_PAYMENT',
}

cx_Oracle.init_oracle_client(lib_dir=r"E:\downloads\instantclient_21_7")

# Connect as user "hr" with password "welcome" to the "orclpdb1" service running on this computer.


def is_modi_important(x, type='IMHR'):
    if x == type:
        return "بلی"
    return "خیر"


def get_ezhar_type(x):
    if x[-5:] == 'TERHR':
        return 'بر اساس تراکنش بانکی'
    elif x[-4:] == 'IMHR':
        return "مودیان مهم با ریسک بالا"
    elif x[-4:] == 'BDLR':
        return 'اظهارنامه های ارزش افزوده مشمول ضوابط اجرایی موضوع بند (ب)تبصره (6) قانون بودجه سال 1400'
    elif x[-4:] == 'T100':
        return 'تبصره 100'
    elif x[-4:] == 'ERHR':
        return 'رتبه ریسک بالا'
    elif x[-4:] == 'MRHR':
        return 'ریسک متوسط'
    elif x[-4:] == 'DMHR':
        return 'اظهارنامه برآوردی صفر'
    elif x[-3:] == 'ZHR':
        return 'اظهارنامه صفر دارای اطلاعات سیستمی'
    elif x[-2:] == 'LR':
        return 'رتبه ریسک پایین'
    elif x[-2:] == 'ZR':
        return 'اظهارنامه صفر فاقد اطلاعات سیستمی/جهت بررسی توسط اداره'
    elif x[-2:] == 'DM':
        return 'اظهارنامه برآوردی غیر صفر'
    elif x[-2:] == 'HR':
        return 'انتخاب شده بدون اعمال معیار ریسک'
    else:
        return 'نامشخص'


def detect_last_condition(x):

    if len(str(x['تاریخ ابلاغ برگ قطعی'])) > 5:
        return 'برگ قطعی ابلاغ شد'
    if len(str(x['شماره برگ قطعی'])) > 5:
        return 'برگ قطعی صادر شد'
    if len(str(x['تاریخ ابلاغ رای تجدید نظر'])) > 5:
        return 'رای تجدید نظر ابلاغ شد'
    if len(str(x['تاریخ رای تجدید نظر'])) > 5:
        return 'رای تجدید نظر صادر شد'
    if len(str(x['تاریخ جلسه تجدید نظر'])) > 5:
        return 'جلسه تجدید تعیین شد'
    if len(str(x['تاریخ اعتراض هیات تجدید نظر'])) > 5:
        return 'اعتراض در هیات تجدید نظر'
    if len(str(x['تاريخ ابلاغ رای بدوی'])) > 5:
        return 'رای بدوی ابلاغ شد'
    if len(str(x['تاریخ رای بدوی'])) > 5:
        return 'رای بدوی صادر شد'
    if len(str(x['تاريخ جلسه بدوی'])) > 5:
        return 'تاریخ جلسه بدوی تعیین شد'
    if len(str(x['تاریخ اعتراض هیات بدوی'])) > 5:
        return 'اعتراض در هیات بدوی'
    if x['توافق'] == 'Y':
        return 'دارای توافق'
    if len(str(x['تاریخ ابلاغ برگ تشخیص'])) > 5:
        return 'برگ تشخیص ابلاغ شد'
    if len(str(x['شماره برگ تشخیص'])) > 5:
        return 'برگ تشخیص صادر شد'
    if len(str(x['تاریخ ایجاد کیس حسابرسی'])) > 5:
        return 'در حال رسیدگی'
    if len(str(x['شماره برگ اجرا'])) > 5:
        return 'اجرائیات'

    return 'فاقد حکم رسیدگی'


def input_info():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tasks', type=str,
                        help='Tasks to be run', default='all')
    parser.add_argument('--RUNALL', type=str,
                        help='Tasks to be run', default='false')
    args = parser.parse_args()

    selected_tasks = args.tasks.split(',')
    RUNALL = args.RUNALL
    return selected_tasks, RUNALL


# tasks, runall = input_info()
tasks = ['all']
RUNALL = 'true'


@time_it(log=True)
def get_allSanim(excel_file=None, selected_sql_query=None, tbl_name=None, del_tbl=True, only_schema=False, df_tax_types=None):
    connection = cx_Oracle.connect(user="ostan_khozestan", password="S_KfvDKu_9851z@hFsTf",
                                   dsn="10.1.1.200:1521/EXTDB")
    cursor = connection.cursor()
    cursor.execute(selected_sql_query)

    if excel_file is None:
        rows = cursor.fetchall()
        return rows[0][0]

    else:
        path = os.path.join(base_path, excel_file)
        # Get the name of columns
        df_mapping = pd.read_excel(path).astype('str')
        df_mapping = dict(rename_duplicates(
            df_mapping, df_mapping.columns.tolist()).to_numpy())
        columns = pd.DataFrame(
            [c[0] for c in cursor.description], columns=['engcol'])
        # if 'RNUM' in columns:
        #     columns = columns[:-1]
        columns.replace(
            {'engcol': df_mapping}, inplace=True)
        columns = list(columns.to_numpy().flatten())

        if del_tbl:

            if tbl_name == 'V_PORTAL':
                columns.append('نوع اظهارنامه')
                columns.append('پرونده مهم')
                columns.append('آخرین وضعیت')
            columns.append('آخرین بروزرسانی')

            sql_delete_query = sql_delete(tbl_name)
            connect_to_sql(sql_query=sql_delete_query,
                           connect_type='dropping sql table %s' % tbl_name)

            sql_create_table_query = create_sql_table(tbl_name, columns)
            connect_to_sql(sql_query=sql_create_table_query,
                           connect_type='creating sql table %s' % tbl_name)

        if only_schema:
            return

        # Fetch data
        num_rows = NUM_ROWS
        while True:
            rows = cursor.fetchmany(num_rows)
            rows = [row[:-1]
                    for row in rows] if 'RNUM' in columns else [row for row in rows]

            if not rows:
                break

            df = pd.DataFrame(rows, columns=columns[:-1])

            def preprocess_data(df, tbl_name):
                search_terms = ['تاریخ', 'مهلت']
                date_cols = []
                df = df.astype(str)
                df_cols = df.columns.tolist()

                # Identify date columns
                for item in df_cols:
                    for search_term in search_terms:
                        if search_term in item:
                            if item not in date_cols:
                                date_cols.append(item)

                df.replace(
                    {'منبع مالیاتی': df_tax_types}, inplace=True)

                if tbl_name == 'V_PORTAL':

                    df['نوع اظهارنامه'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].apply(
                        lambda x: get_ezhar_type(str(x)))

                    df.replace('nan', 0, inplace=True)
                    df.replace('None', 0, inplace=True)
                    df.replace('NaN', 0, inplace=True)
                    df.replace('NaT', 0, inplace=True)

                    df['سال عملکرد'] = df['سال عملکرد'].apply(
                        np.float64).replace(np.nan, 0).apply(np.int64)

                    df.replace(np.nan, 0, inplace=True)

                    df.fillna(0, inplace=True)

                    df['شماره اظهارنامه'] = df['شماره اظهارنامه'].apply(
                        np.float64).apply(np.int64)
                    df['دوره عملکرد'] = df['دوره عملکرد'].apply(
                        np.float64).apply(np.int64)
                    df['شناسه حسابرسی'] = df['شناسه حسابرسی'].apply(
                        np.float64).apply(np.int64)
                    df['شماره برگ قطعی'] = df['شماره برگ قطعی'].apply(
                        np.float64).apply(np.int64)
                    df['شماره برگ اجرا'] = df['شماره برگ اجرا'].apply(
                        np.float64).apply(np.int64)
                    df['عوارض قطعی'] = df['عوارض قطعی'].apply(
                        np.float64).apply(np.int64)
                    df['پرداختی از اصل مالیات'] = df['پرداختی از اصل مالیات'].apply(
                        np.float64).apply(np.int64)
                    df['میزان مبلغ بخشودگی'] = df['میزان مبلغ بخشودگی'].apply(
                        np.float64).apply(np.int64)
                    df['پرداختی از اصل مالیات'] = df['پرداختی از اصل مالیات'].apply(
                        np.float64).apply(np.int64)
                    df['مالیات تشخیص'] = df['مالیات تشخیص'].apply(
                        np.float64).apply(np.int64)
                    df['درآمد تشخیص'] = df['درآمد تشخیص'].apply(
                        np.float64).apply(np.int64)
                    df['فروش تشخیص'] = df['فروش تشخیص'].apply(
                        np.float64).apply(np.int64)
                    df['فروش قطعی'] = df['فروش قطعی'].apply(
                        np.float64).apply(np.int64)
                    df['مالیات قطعی'] = df['مالیات قطعی'].apply(
                        np.float64).apply(np.int64)
                    df['درآمد قطعی'] = df['درآمد قطعی'].apply(
                        np.float64).apply(np.int64)
                    df['مالیات ابرازی'] = df['مالیات ابرازی'].apply(
                        np.float64).apply(np.int64)
                    df['درآمد ابرازی'] = df['درآمد ابرازی'].apply(
                        np.float64).apply(np.int64)
                    df['عوارض ابرازی'] = df['عوارض ابرازی'].apply(
                        np.float64).apply(np.int64)
                    df['عوارض تشخیص'] = df['عوارض تشخیص'].apply(
                        np.float64).apply(np.int64)
                    df['فروش ابرازی'] = df['فروش ابرازی'].apply(
                        np.float64).apply(np.int64)
                    df['سود و زیان ابرازی'] = df['سود و زیان ابرازی'].apply(
                        np.float64).apply(np.int64)
                    df['مانده بدهی از اصل مالیات'] = df['مانده بدهی از اصل مالیات'].apply(
                        np.float64).apply(np.int64)
                    df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'] = df[
                        'کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].astype(str)

                    df['پرونده مهم'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].apply(
                        lambda x: is_modi_important(str(x[-4:]), type='IMHR'))

                    df['آخرین وضعیت'] = df.apply(
                        detect_last_condition, axis=1)

                df['آخرین بروزرسانی'] = get_update_date()

                if date_cols:
                    for item in date_cols:
                        df[item] = df[item].astype(str)
                        df[item] = df[item].apply(
                            lambda x: is_date(x))

                return df

            df = preprocess_data(df, tbl_name)
            sql_insert = insert_into(tbl_name, df.columns.tolist())
            connect_to_sql(sql_query=sql_insert, df_values=df.values.tolist(
            ), connect_type='inserting into sql table')

        cursor.close()
        connection.close()


# Determine number of buckets and threads
@log_the_func('none')
def count_tbls(tbl=tbl_names['allusers'], num=20000):
    if tbl != 's200_codes_22':
        ostan_txt = '_khozestan'
    else:
        ostan_txt = ''
    count = get_allSanim(None, get_sql_sanim_count(
        tbl, ostan_txt=ostan_txt), None)
    num_threads = math.ceil(count/num)
    buckets = []
    tmp = 0
    c_num = num
    while count > num:
        buckets.append((tmp, num))
        tmp = num + 1
        num += c_num
    if num > count:
        buckets.append((tmp, count))
    return buckets, num_threads

# Back up function which runs afer the tables have been updated.


def backup_func():
    for key, value in tbl_names.items():
        sql_query = 'SELECT * INTO %s FROM [TestDb].[dbo].[%s]' % (
            value, value)
        drop_into_db(value, append_to_prev=False, db_name='testdbV2',
                     create_tbl='no', sql_query=sql_query)
        sql_query = 'ALTER TABLE %s ADD PRIMARY KEY (ID)' % value
        connect_to_sql(sql_query, get_sql_con(database='testdbV2'))


def run_thread(job_func, *args, **kwargs):
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()


params = {'allusers': [excel_files['allusers'], get_sql_sanimusers(), tbl_names['allusers']],
          'allsanim': [excel_files['allsanim'], get_sql_allsanim(), tbl_names['allsanim']],
          'allbakhshodegi': [excel_files['allbakhshodegi'], get_sql_allbakhshodegi(), tbl_names['allbakhshodegi']],
          'alleterazat': [excel_files['alleterazat'], get_sql_alleterazat(), tbl_names['alleterazat']],
          'allhesabdari': [excel_files['allhesabdari'], get_sql_allhesabdari(), tbl_names['allhesabdari']],
          'allanbare': [excel_files['allanbare'], get_sql_allanbare(), tbl_names['allanbare']],
          'allejra': [excel_files['allejra'], get_sql_allejra(), tbl_names['allejra']],
          'allhesabrasi': [excel_files['allhesabrasi'], get_sql_allhesabrasi(), tbl_names['allhesabrasi']],
          'allpayment': [excel_files['allpayment'], get_sql_allpayment(), tbl_names['allpayment']],
          'allcodes': [excel_files['allcodes'], get_sql_allcodes(), tbl_names['allcodes']],
          }

# The main fuction


@log_the_func('none')
def run_it(buckets=None, num_threads=None, field=None):
    df_tax_types = get_tax_types()
    for key, value in tbl_names.items():
        if key != 'allcodes':
            ostan_txt = '_khozestan'
        else:
            ostan_txt = ''

        # Create schema
        get_allSanim(excel_files[key], get_sql_sanimusers(
            tblname=value, ostan_txt=ostan_txt), value, only_schema=True)
        # Specify number of threads and buckets
        num_threads = 0
        while (num_threads <= 0):
            buckets, num_threads = count_tbls(tbl=value)
        executor = ThreadPoolExecutor(num_threads)
        jobs = [executor.submit(get_allSanim, excel_files[key],
                                get_sql_sanimusers(
                                    item[0], item[1], value, ostan_txt),
                                value, False, False, df_tax_types)
                for item in buckets]
        wait(jobs)

    backup_func()
    # Report section
    # rpt_tashkhis_ghatee_238(
    #     years=['1401.0', '1400.0', '1399.0', '1398.0', '1397.0', '1396.0', '1395.0'])

    # get_important_modis(
    #     years=['1401.0', '1400.0', '1399.0', '1398.0', '1397.0', '1396.0', '1395.0'], drop_db=True, group_by=None,
    #     path_dir=None)

    # run_it()

    # Scheduling the main function


def schedule_tasks(time_to_run='15:30'):

    if 'all' in tasks:
        schedule.every().day.at(time_to_run).do(run_it)

    if '1' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allusers'][0], params['allusers'][1], params['allusers'][2])
    if '2' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allsanim'][0], params['allsanim'][1], params['allsanim'][2])
    if '3' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allbakhshodegi'][0], params['allbakhshodegi'][1], params['allbakhshodegi'][2])
    if '4' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['alleterazat'][0], params['alleterazat'][1], params['alleterazat'][2])
    if '5' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allhesabdari'][0], params['allhesabdari'][1], params['allhesabdari'][2])
    if '6' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allanbare'][0], params['allanbare'][1], params['allanbare'][2])
    if '7' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allhesabrasi'][0], params['allhesabrasi'][1], params['allhesabrasi'][2])
    if '8' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allejra'][0], params['allejra'][1], params['allejra'][2])
    if '9' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allpayment'][0], params['allpayment'][1], params['allpayment'][2])
    if '10' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allcodes'][0], params['allcodes'][1], params['allcodes'][2])

    while True:
        if RUNALL == 'true':
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            # schedule.run_all(delay_seconds=10)
            time.sleep(1)


def run_without_scheduling():

    get_allSanim(excel_files['allsanim'], get_sql_allsanim(
    ), tbl_names['allsanim'])

    get_allSanim(excel_files['allejra'], get_sql_allejra(
    ), tbl_names['allejra'])

    get_allSanim(excel_files['allusers'], get_sql_sanimusers(
    ), tbl_names['allusers'])

    get_allSanim(excel_files['alleterazat'], get_sql_alleterazat(
    ), tbl_names['alleterazat'])

    get_allSanim(excel_files['allbakhshodegi'], get_sql_allbakhshodegi(
    ), tbl_names['allbakhshodegi'])

    get_allSanim(excel_files['allhesabdari'], get_sql_allhesabdari(
    ), tbl_names['allhesabdari'])

    get_allSanim(excel_files['allanbare'], get_sql_allanbare(
    ), tbl_names['allanbare'])

    get_allSanim(excel_files['allhesabrasi'], get_sql_allhesabrasi(
    ), tbl_names['allhesabrasi'])

    get_allSanim(excel_files['allpayment'], get_sql_allpayment(
    ), tbl_names['allpayment'])

    get_allSanim(excel_files['allcodes'], get_sql_allcodes(
    ), tbl_names['allcodes'])


if __name__ == '__main__':
    print('Starting the schedule...')
    schedule_tasks(time_to_run=TIME_TO_RUN)

    # run_without_scheduling()

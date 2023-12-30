from time import sleep
import functools as ft
import time
import click
import os
import glob
import pandas as pd
import numpy as np
from helpers import maybe_make_dir, count_num_files, \
    insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func, wrap_it_with_params
from scrape import Scrape
from constants import get_sql_con, Role
import schedule
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor

TIME_TO_RUN = '05:00'
PROMPT = True
END_TIME = 20
RUN_ALL = True
MULTI_THREADING = False
NUM_THREADS = 3
ROLE = Role.manager_phase1.name
MULTI_THREADING_PATH = r'C:\Users\alkav\Documents\file\files'
HEADLESS = False
TIME_OUT = 160
SAVE_EVERY_MAIN = 10
SAVE_EVERY_BACK = 30
SHENASE_DADRASI = 'no'
INFO = {'success': True, 'keep_alive': True}

project_root = os.path.dirname(os.path.dirname(__file__))

PATH_CODEGHTSADI = os.path.join(project_root, r'saved_dir\codeghtesadi')

maybe_make_dir([PATH_CODEGHTSADI])

PATH = r'C:\Users\alkav\Documents\file\files'
BACKUP_PATH = r'C:\Users\alkav\Documents\file\files\backup'

DATA_FILE_PATH = r'C:\Users\alkav\Documents\file\files\file0.xlsx'
DATA_FILE_PATH_FINAL = r'C:\Users\alkav\Documents\file\file.xlsx'
USERS_FILE_PATH = r'C:\Users\alkav\Documents\file\users.xlsx'

DF_USERS = pd.read_excel(USERS_FILE_PATH)


def run_it(df=None,
           path=PATH,
           file_name=None,
           backup_file=None,
           time_out=TIME_OUT,
           save_every_main=SAVE_EVERY_MAIN,
           save_every_back=SAVE_EVERY_BACK,
           shenase_dadrasi=SHENASE_DADRASI,
           *args, **kwargs):

    x = Scrape()
    x.scrape_iris(path=PATH,
                  df=df,
                  del_prev_files=False,
                  headless=HEADLESS,
                  file_name=file_name,
                  backup_file=backup_file,
                  time_out=TIME_OUT,
                  save_every_main=SAVE_EVERY_MAIN,
                  save_every_back=SAVE_EVERY_BACK,
                  role=ROLE,
                  end_time=END_TIME,
                  shenase_dadrasi=SHENASE_DADRASI,
                  info=INFO)

# @wrap_it_with_params(3, 1000000000)


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={}):

    schedule.every().day.at(time_to_run).do(
        run_it,
        run_it_params['df'],
        run_it_params['path'],
        run_it_params['file_name'],
        run_it_params['backup_file'],
        run_it_params['time_out'],
        run_it_params['save_every_main'],
        run_it_params['save_every_back'],
        run_it_params['role'],
        run_it_params['end_time'],
        run_it_params['shenase_dadrasi'],
        run_it_params['info'],
    )

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


@click.command()
@click.option('--multi_threading', prompt=PROMPT, default=MULTI_THREADING)
@click.option('--time_to_run', prompt=PROMPT, default=TIME_TO_RUN)
@click.option('--time_out', prompt=PROMPT, default=TIME_OUT)
@click.option('--headless', prompt=PROMPT, default=HEADLESS)
@click.option('--save_every_main', prompt=PROMPT, default=SAVE_EVERY_MAIN)
@click.option('--save_every_back', prompt=PROMPT, default=SAVE_EVERY_BACK)
@click.option('--role', prompt=PROMPT, default=ROLE)
@click.option('--end_time', prompt=PROMPT, default=END_TIME)
@click.option('--shenase_dadrasi', prompt=PROMPT, default=SHENASE_DADRASI)
def Get_Params(*args, **kwargs):
    return kwargs


def get_dfs_data(multi_threading):

    # Split data into multiple files
    file_list = glob.glob(PATH + "/*" + 'xlsx')
    file_list_backup = glob.glob(BACKUP_PATH + "/*" + 'xlsx')

    if multi_threading:
        maybe_make_dir([MULTI_THREADING_PATH])
        DFS_USERS = np.array_split(DF_USERS, NUM_THREADS)

        dfs = []
        dfs_data = []

        # for item in zip(DFS_DATA, DFS_USERS):
        #     dfs.append(item)

        if len(file_list) == 0:
            df_data = pd.read_excel(DATA_FILE_PATH)
            df_data = np.array_split(df_data, NUM_THREADS)
            for ind, item in enumerate(df_data):
                item.to_excel(
                    r'C:\Users\alkav\Documents\file\files\file%s.xlsx' % ind, index=False)
                dfs_data.append([item, DFS_USERS[ind]])

            file_list = glob.glob(PATH + "/*" + 'xlsx')
        else:
            for ind, item in enumerate(file_list):
                dfs_data.append([pd.read_excel(item), DFS_USERS[ind]])

    else:

        df_data = pd.read_excel(DATA_FILE_PATH)
        dfs_data = []
        dfs_data.append([df_data, DF_USERS])

    return dfs_data, file_list, file_list_backup


if __name__ == '__main__':

    args = Get_Params.main(standalone_mode=False)

    dfs_data, file_list, file_list_backup = get_dfs_data(
        args['multi_threading'])

    if args['multi_threading']:
        # executor = ThreadPoolExecutor(NUM_THREADS)
        executor = ProcessPoolExecutor(NUM_THREADS)

        with ProcessPoolExecutor(NUM_THREADS) as executor:
            jobs = [executor.submit(schedule_tasks, args['time_to_run'],
                                    RUN_ALL, {'df': item,
                                              'path': PATH,
                                              'file_name': file_list[index],
                                              'backup_file': file_list_backup[index],
                                              'time_out': args['time_out'],
                                              'role': args['role'],
                                              'save_every_main': args['save_every_main'],
                                              'save_every_back': args['save_every_back'],
                                              'end_time': args['end_time'],
                                              'shenase_dadrasi': args['shenase_dadrasi'],
                                              'info': INFO,
                                              })
                    for index, item in enumerate(dfs_data)]
            wait(jobs)

    else:
        schedule_tasks(time_to_run=args['time_to_run'], run_it_params={
            'df': dfs_data[0],
            'path': PATH,
            'file_name': file_list[0],
            'backup_file': file_list_backup[0],
            'time_out': args['time_out'],
            'role': args['role'],
            'save_every_main': args['save_every_main'],
            'save_every_back': args['save_every_back'],
            'end_time': args['end_time'],
            'shenase_dadrasi': args['shenase_dadrasi'],
            'info': INFO
        })

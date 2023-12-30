import time
import os
from datetime import datetime, timedelta
from helpers import maybe_make_dir, input_info, \
    remove_excel_files, is_updated_to_download, \
    is_updated_to_save, log_the_func
from dump_to_sql import DumpToSQL
from sql_queries import get_table_names, sql_delete
from scrape import Scrape
import schedule
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor
import multiprocessing

TIME_OUT = 5
type_one_reports = ['ezhar', 'hesabrasi_darjarian_before5', 'hesabrasi_darjarian_after5', 'hesabrasi_takmil_shode',
                    'tashkhis_sader_shode', 'tashkhis_eblagh_shode', 'ghatee_sader_shode', 'ghatee_eblagh_shode', '1000_parvande']
type_two_repors = ['badvi_darjarian_dadrasi', 'badvi_takmil_shode',
                   'tajdidnazer_darjarian_dadrasi', 'tajdidnazar_takmil_shode',
                   'badvi_darjarian_dadrasi_hamarz', 'amar_sodor_gharar_karshenasi', 'amar_sodor_ray']
type_of_reports = {'type_one': type_one_reports, 'type_two': type_two_repors}

excel_file_names = ['Excel.xlsx', 'Excel(1).xlsx', 'Excel(2).xlsx']
excel_file_names_type_two = [
    'جزئیات اعتراضات و شکایات.html']

report_types, years, initial_scrape, initial_dump_to_sql, initial_create_reports = input_info()

NUM_THREADS = len(years)
HEADLESS = True
RUN_ALL = True
TIME_TO_RUN = '15:17:33'

MULTI_THREADING = True
executor = ProcessPoolExecutor(NUM_THREADS)


@log_the_func('none')
def run_scrape(year, report_types, headless=HEADLESS, time_out=TIME_OUT, lock=None):
    global initial_scrape
    global initial_dump_to_sql
    global initial_create_reports

    for report_type in report_types:

        print('dump_to_sql = %s and create_reports = %s and scrape = %s' %
              (initial_dump_to_sql, initial_create_reports, initial_scrape))

        table_names = get_table_names(report_type, year)
        is_saved = is_updated_to_download(
            table_name=table_names[0], type_of='save')

        if (is_saved):
            continue

        if not is_saved:
            for key, values in type_of_reports.items():
                for value in values:
                    if value == report_type:
                        type_of_report = key

            exists_in_type_one_repors = type_one_reports.count(report_type)

            scrape = initial_scrape
            dump_to_sql = initial_dump_to_sql
            create_reports = initial_create_reports
            path = r'C:\ezhar-temp\%s\%s' % (year, report_type)
            maybe_make_dir([path])

            # Update the reports
            print('updating excel files...................................')

            excel_sherkatha = '%s\%s' % (path, excel_file_names[0])
            excel_mashaghel = '%s\%s' % (path, excel_file_names[1])
            excel_arzeshafzoode = '%s\%s' % (path, excel_file_names[2])
            excel_badvi = '%s\%s' % (path, excel_file_names_type_two[0])

            # download excel files
            if (scrape == 's' and type_of_report == 'type_one'):

                # hoghoghi_updated = is_updated_to_download(
                #     table_name=table_names[1], type_of='download')
                # haghighi_updated = is_updated_to_download(table_name=table_names[0],
                #                                           type_of='download')
                # arzeshafzoode_updated = is_updated_to_download(
                #     table_name=table_names[2], type_of='download')

                # if (os.path.exists(excel_sherkatha) and not (hoghoghi_updated)):
                #     remove_excel_files([excel_sherkatha])

                # if (os.path.exists(excel_mashaghel) and not (haghighi_updated)):
                #     remove_excel_files([excel_mashaghel])

                # if (os.path.exists(excel_arzeshafzoode) and not (arzeshafzoode_updated)):
                #     remove_excel_files([excel_arzeshafzoode])

                if (os.path.exists(excel_sherkatha)):
                    remove_excel_files([excel_sherkatha])

                if (os.path.exists(excel_mashaghel)):
                    remove_excel_files([excel_mashaghel])

                if (os.path.exists(excel_arzeshafzoode)):
                    remove_excel_files([excel_arzeshafzoode])

                # if ((hoghoghi_updated) and
                #     (haghighi_updated) and
                #         (arzeshafzoode_updated)):

                #     print('All excel files related to %s year %s are up to date' % (
                #         report_type, year))
                #     scrape = 'not-s'

            elif (scrape == 's' and type_of_report == 'type_two'):

                badvi_updated = is_updated_to_download(
                    table_name=table_names[0], type_of='download')

                if (os.path.exists(excel_badvi) and not (badvi_updated)):
                    remove_excel_files([excel_badvi])

                if (badvi_updated):

                    print('All excel files related to %s year %s are up to date' % (
                        report_type, year))
                    scrape = 'not-s'

            if scrape == 's':
                type_of = 'download'
                x = Scrape(path=path, report_type=report_type,
                           year=year, headless=headless, time_out=time_out, table_name=table_names[0], type_of=type_of)
                x.scrape_sanim()

            # Dump excel files into sql table

            if (dump_to_sql == 'd' and type_of_report == 'type_one'):
                ...

            elif (dump_to_sql == 'd' and type_of_report == 'type_two'):

                if (is_updated_to_download(
                        table_name=table_names[0], type_of='save')):
                    print('All excel files related to %s year %s are saved' %
                          (report_type, year))
                    dump_to_sql = 'not-d'

            if (dump_to_sql == 'd'):

                sql_delete_script = sql_delete(table_names[0])

                dump = DumpToSQL(report_type=report_type, table=table_names[0], year=year,
                                 sql_delete=sql_delete_script, path=path, type_of_report=type_of_report)
                dump.dump_to_sql(table_name=table_names[0], type_of='save')

            if (create_reports == 'c'):
                dump = DumpToSQL(report_type=report_type,
                                 table=table_names[0], year=year, path=path)
                dump.create_anbare_reports(year)
                dump.create_Anbare99Mashaghel_reports(year)
                dump.create_Anbare99Sherkatha_reports(year)
                dump.create_hesabrasiArzeshAfzoode_reports(year)


def schedule_tasks(time_to_run=TIME_TO_RUN, run_all=RUN_ALL, run_scrape_params={}):

    schedule.every().day.at(time_to_run).do(run_scrape,
                                            run_scrape_params['year'],
                                            run_scrape_params['report_types'],
                                            run_scrape_params['headless'],
                                            run_scrape_params['time_out'],
                                            run_scrape_params['lock']
                                            )

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":

    if MULTI_THREADING:

        final_times = []
        date1 = datetime.strptime(TIME_TO_RUN, '%H:%M:%S')

        for year in years:
            final_times.append(f'{date1.hour:02}:{date1.minute:02}')
            date1 = date1 + timedelta(seconds=60)

        m = multiprocessing.Manager()
        lock = m.Lock()
        with ProcessPoolExecutor(NUM_THREADS) as executor:

            jobs = [executor.submit(schedule_tasks, item[1],
                                    RUN_ALL, {'year': item[0],
                                              'report_types': report_types,
                                              'headless': HEADLESS,
                                              'time_out': TIME_OUT,
                                              'lock': lock
                                              })
                    for index, item in enumerate(zip(years, final_times))]

            wait(jobs)

    else:
        print('waiting for the schedule to start')
        schedule_tasks(time_to_run=TIME_TO_RUN)

    # schedule.every().day.at(time_to_run).do(run_scrape)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

    # sys.exit()

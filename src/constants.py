import os
from selenium import webdriver
import socket
from dataclasses import dataclass
from enum import Enum


from enum import Enum


class Role(Enum):
    """
    Enum class representing different roles.

    Attributes:
        manager_phase1: Role for Manager in Phase 1
        manager_phase2: Role for Manager in Phase 2
        employee: Role for Employee
    """
    manager_phase1 = 1
    manager_phase2 = 3
    employee = 2


@dataclass
class Modi():
    """
    Dataclass representing information about an individual.

    Attributes:
        melli: National ID (str)
        sex: Gender (str)
        name: Full name (str)
        father_name: Father's name (str)
        dob: Date of birth (str)
        id_num: Identification number (str)
    """
    melli: str = None
    sex: str = None
    name: str = None
    father_name: str = None
    dob: str = None
    id_num: str = None


@dataclass
class ModiHoghogh():
    """
    Dataclass representing salary and benefit information.

    Attributes:
        melli_code: National ID (str)
        name: Full name (str)
        sur_name: Surname (str)
        start_work_date: Start date of employment (str)
        sum_ongoing_gross_salary_rial: Sum of ongoing gross salary in Rials (str)
        # ... (other attributes omitted for brevity)
        net_tax: Net tax amount (str)
    """
    # ... (attributes omitted for brevity)


@dataclass
class ModiHoghoghLst():
    """
    Dataclass representing a list of employee information.

    Attributes:
        melli_code: National ID (str)
        name: Full name (str)
        sur_name: Surname (str)
        position: Job position (str)
    """
    melli_code: str = None
    name: str = None
    sur_name: str = None
    position: str = None


@dataclass
class Modi_pazerande():
    """
    Dataclass representing information about a payer.

    Attributes:
        melli: National ID (str)
        hoviyati: Title (str)
        code: Code (str)
        shomarepayane: Invoice number (str)
        vaziat: Status (str)
        vaziatelsagh: Status details (str)
        money1400: Amount for the year 1400 (str)
        money1401: Amount for the year 1401 (str)
    """
    # ... (attributes omitted for brevity)


scrape_from = {'sanim': 'sanim',
               'arzeshafzoodeh': 'arzeshafzoodeh'}


def get_months():
    """
    Function to get a list of months in string format.

    Returns:
        List of months (List[str])
    """
    months = ['01', '02', '03', '04', '05',
              '06', '07', '08', '09', '10', '11', '12']
    return months


def get_table_names():
    """
    Function to get a list of table names.

    Returns:
        List of table names (List[str])
    """
    table_names = [...]
    return table_names  # List of table names omitted for brevity


def get_arzeshafzoodeNames():
    """
    Function to get a dictionary of arzeshafzoode names and corresponding years.

    Returns:
        Dictionary of arzeshafzoode names and years (Dict[str, str])
    """
    arzeshafzoode_names = {...}
    return arzeshafzoode_names  # Dictionary of names and years omitted for brevity


def get_sql_con(server='.', database='testdb', username='sa', password='14579Ali'):

    constr = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + \
        database + ';UID=' + username + ';PWD=' + password

    return constr


def get_arzeshafzoodeNames():
    """
    Function to get a dictionary of arzeshafzoode names and corresponding years.

    Returns:
        Dictionary of arzeshafzoode names and years (Dict[str, str])
    """
    arzeshafzoode_names = {...}
    return arzeshafzoode_names  # Dictionary of names and years omitted for brevity


def get_server_namesV2():
    """
    Function to get a list of server names and their corresponding addresses.

    Returns:
        List of server names and addresses (List[Tuple[str, str]])
    """
    server_names = [...]
    return server_names  # List of server names omitted for brevity


def get_report_links(report_type: str):
    """
    Function to get a list of report links based on the report type.

    Args:
        report_type: Type of report (str)

    Returns:
        List of report links (List[str])
    """
    if report_type == 'ezhar':
        return [...]
    elif report_type == 'tashkhis_sader_shode':
        return [...]
    # ... (other conditions omitted for brevity)
    else:
        return []


lst_years_arzeshafzoodeSonati = {
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited.xls': '1387',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(1).xls': '1388',
    # ... (other entries omitted for brevity)
}

years = [
    ('all', '0'),
    ('common_years', '1'),
    ('1392', '92'),
    ('1393', '93'),
    # ... (other entries omitted for brevity)
]

dict_years = {
    'all': '0',
    'common_years': '1',
    '1387': '87',
    '1388': '88',
    # ... (other entries omitted for brevity)
}


def get_dict_years():
    """
    Function to get a dictionary of years and their codes.

    Returns:
        Dictionary of years and codes (Dict[str, str])
    """
    return dict_years


all_years = years[0][1]
common_years = years[1][1]


def get_all_years():
    """
    Function to get the code for all years.

    Returns:
        Code for all years (str)
    """
    return all_years


def get_common_years():
    """
    Function to get the code for common years.

    Returns:
        Code for common years (str)
    """
    return common_years


lst_reports = [
    ('common_reports', '0'),
    ('heiat_reports', '00'),
    ('ezhar', '1'),
    ('hesabrasi_darjarian_before5', '2'),
    # ... (other entries omitted for brevity)
]

comm_reports = lst_reports[0][1]
heiat = lst_reports[1][1]


def get_comm_reports():
    """
    Function to get the code for common reports.

    Returns:
        Code for common reports (str)
    """
    return comm_reports


def get_heiat():
    """
    Function to get the code for heiat reports.

    Returns:
        Code for heiat reports (str)
    """
    return heiat


common_reports = ['ezhar', 'tashkhis_sader_shode',
                  'tashkhis_eblagh_shode', 'ghatee_sader_shode', 'ghatee_eblagh_shode']
heiat_reports = lst_reports[12:16]


def get_common_reports():
    """
    Function to get a list of common reports.

    Returns:
        List of common reports (List[str])
    """
    return common_reports


common_years = years[5:]
comm_years = years[1][1]


def get_common_years():
    """
    Function to get common years.

    Returns:
        Common years (List[Tuple[str, str]])
    """
    return common_years


def get_comm_years():
    """
    Function to get the code for common years.

    Returns:
        Code for common years (str)
    """
    return comm_years


def get_common_reports():
    """
    Function to get a list of common reports.

    Returns:
        List of common reports (List[str])
    """
    return common_reports


def get_lst_reports():
    """
    Function to get a list of all reports.

    Returns:
        List of all reports (List[Tuple[str, str]])
    """
    return lst_reports


def get_years():
    """
    Function to get a list of years.

    Returns:
        List of years (List[Tuple[str, str]])
    """
    return years[2:]


def get_str_years():
    """
    Function to get a formatted string of years and their codes.

    Returns:
        Formatted string of years and codes (str)
    """
    str_years = 'types:\n'
    for year in years:
        str_years += '%s=%s\n' % (year[0], year[1])

    return str_years


def get_str_help():
    """
    Function to get a formatted string of help information for report types.

    Returns:
        Formatted string of help information (str)
    """
    str_help = 'Report types:'
    for item in lst_reports:
        str_help += '%s=%s,\n' % (item[0], item[1])

    return str_help


def get_ip():
    """
    Function to get the IP address of the host machine.

    Returns:
        IP address (str)
    """
    host_name = socket.gethostname()
    ip_addr = socket.gethostbyname(host_name)

    return ip_addr


def geck_location(set_save_dir=False, driver_type='firefox'):
    """
    Function to get the location of the geckodriver or the save directory.

    Args:
        set_save_dir: Boolean flag to indicate if the save directory is set (default: False)
        driver_type: Type of driver ('firefox' or 'chrome') (default: 'firefox')

    Returns:
        Location of geckodriver or save directory (str)
    """
    # ... (function implementation omitted for brevity)


def set_gecko_prefs(pathsave):
    """
    Function to set preferences for geckodriver.

    Args:
        pathsave: Path to the save directory (str)

    Returns:
        FirefoxProfile object with set preferences
    """
    fp = webdriver.FirefoxProfile()
    # ... (function implementation omitted for brevity)
    return fp


def get_heiat_reports():
    """
    Function to get a list of heiat reports.

    Returns:
        List of heiat reports (List[str])
    """
    return heiat_reports


def get_remote_sql_con():
    server = '10.52.0.114'
    database = 'TestDb'
    username = 'sa'
    password = '14579Ali'
    constr = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + \
        database + ';UID=' + username + ';PWD=' + password

    return constr


def get_soratmoamelat_mapping():
    return {
        'اطلاعات کارفرما در پيمان هاي بلند مدت': 'tblSorammoamelatKarfarmaPeymanBoland',
        'اجاره': 'tblSorammoamelatEjare',
        'فروش': 'tblSorammoamelatForosh',
        'اطلاعات پيمانکار در پيمان هاي بلند مدت': 'tblSorammoamelatPeymankarPeymanBoland',
        'خريد': 'tblSorammoamelatKharid',
        'فعاليت هاي ساخت و پيش فروش املاک': 'tblSorammoamelatSakhtPishforoshAmlak',
        'صادرات/فروش به شخص خارجي': 'tblSorammoamelatSaderatBeShakhsKhareji',
        'صادرات': 'tblSorammoamelatSaderatBeShakhsKhareji',
        'واردات/خريد از شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'واردات': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'حق العملکاري - کارمزد - فروشنده': 'tblSorammoamelatHagholamalKariKarmozdForoshande',
        'حق العملکاري - کارمزد - خريدار/کارفرما': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'حق العملکاري - کارمزد - خریدار': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'حق العملکاري - صاحب کالا - فروشنده': 'tblSorammoamelatHagholamalKariSahebkalaForoshande',
        'حق العملکاري - خريدار/کارفرما': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - خریدار': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - خريدار': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - کارمزد - خريدار': 'tblSorammoamelatHagholamalKariSahebkalaForoshande',
        'کارفرما': 'tblSorammoamelatKharidarKarfarma',
        'فروش به شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'خريد از شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'کارفرما2': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'بازرگان': 'tblGomrokBazargan',
        'حق العملکار/ترخیص کننده': 'tblGomrokHagholamalkarTarkhisKonande',
        'ارز': 'tblSayerArz',
        'تأمين کنندگان ارز': 'tblSayerTaminkonandeganArz',
        'تدارکات الکترونيکي': 'tblSayerTadarokatElectonic',
        'تسهيلات بانکي': 'tblSayerTashilatBanki',
        'تعهدات بانکي': 'tblSayerTahodatBanki',
        'خريدار بورس': 'tblSayerKharidarBors',
        'دریافت کنندگان ارز': 'tblSayerDaryaftkonnadeganArz',
        'سکه و طلا و جواهرات،کارشناسان رسمي': 'tblSayerSekkeVaTalaVaJaheratKarshenasanRasmi',
        'فروشنده بورس': 'tblSayerForoshandeBors',
        'مبایعه نامه های صادره بنگاه های معاملات املاک': 'tblSayerMobayenameSadereBongah',
        'وکالت نامه - دفتریار': 'tblSayerVekalatNameDaftaryar',
        'وکالت نامه - سردفتر': 'tblSayerVekalatNameSarDaftar',
        'وکالت نامه - طرفین سند حقوقی': 'tblSayerVekalatNameTarafinSanadHoghghi',
        'وکالتنامه-موکل': 'tblSayerVekalatNameMovakel',
        'وکالتنامه-وکيل': 'tblSayerVekalatNameVakil',
        'وکيل بورس': 'tblSayerVakilBors',
        'کارفرما قرارداد': 'tblSayerKarfarmaGharardad',
        'کارگزار خريدار بورس': 'tblSayerKargozarKharidarBors',
        'کارگزار ': 'tblSayerKargozarForoshandeBors',
        'پيمانکار قرارداد': 'tblSayerPeymankarGharardad',

    }


def get_soratmoamelat_mapping_address():
    return {
        'tblGomrokBazargan': 'آدرس بازرگان',
        'tblGomrokHagholamalkarTarkhisKonande': 'آدرس ترخیص کننده',
        'tblSorammoamelatEjare': 'آدرس طرف قرارداد',
        'tblSorammoamelatForosh': 'آدرس خریدار',
        'tblSorammoamelatHagholamalKariKarmozdForoshande': 'آدرس صاحب کالا(فروشنده)',
        'tblSorammoamelatHagholamalKariKarmozdKharidar': 'آدرس خریدار/کارفرما(مدیریت پیمان)',
        'tblSorammoamelatHagholamalKariSahebkalaForoshande': 'آدرس صاحب کالا(فروشنده)',
        'tblSorammoamelatKarfarmaPeymanBoland': 'آدرس کارفرما',
        'tblSorammoamelatKharid': 'آدرس فروشنده',
        'tblSorammoamelatKharidarKarfarma': 'آدرس خریدار/کارفرما(مدیریت پیمان)',
        'tblSorammoamelatPeymankarPeymanBoland': 'آدرس پیمانکار',
        'tblSorammoamelatSaderatBeShakhsKhareji': '',
        'tblSorammoamelatSakhtPishforoshAmlak': 'آدرس طرف قرارداد',
        'tblSorammoamelatVaredatAzShakhsKhareji': 'آدرس',
    }


def get_url186(name, fromdate, todate):
    urls = {'bankdarhalepeigiri': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=1&rwndrnd=0.391666473501828'.format(fromdate, todate),
            'banksodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(
                fromdate, todate),
            'bankelambedehi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.19619883107398017'.format(
                fromdate, todate),
            'bankadamsabeghe': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.9409760878581537'.format(
                fromdate, todate),
            'bankadameraesoratmali': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(
                fromdate, todate),
            'bankadameraesoratmalielambedehi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=5&Edare=undefined&reqtl=1&rwndrnd=0.12348184643733573'.format(
                fromdate, todate),
            'bankdarkhasttekrari': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=6&Edare=undefined&reqtl=1&rwndrnd=0.9987754619692941'.format(
                fromdate, todate),
            'banknaghseetaleat': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=1&rwndrnd=0.2656927736407235'.format(
                fromdate, todate),
            'asnafdarhalepeigiri': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=3&rwndrnd=0.2053884823939629'.format(
                fromdate, todate),
            'asnafsodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(
                fromdate, todate),
            'asnafadamemkansodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.7202288185326258'.format(
                fromdate, todate),
            'bankbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.6731190588613609'.format(
                fromdate, todate),
            'bankpasokhdarmohlatmogharar': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(
                fromdate, todate),
            'bankpasokhbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.3154066478901216'.format(
                fromdate, todate),
            'asnafbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.6972614531535117'.format(
                fromdate, todate),
            'asnafpasokhdarmohlatmogharar': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(
                fromdate, todate),
            'asnafpasokhbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=9&Edare=undefined&reqtl=3&rwndrnd=0.33102715206520283'.format(
                fromdate, todate)
            }

    return urls[name]


def get186_titles():
    return ['bankdarhalepeigiri',
            'banksodorgovahi',
            'bankelambedehi',
            'bankadamsabeghe',
            'bankadameraesoratmali',
            'bankadameraesoratmalielambedehi',
            'bankdarkhasttekrari',
            'banknaghseetaleat',
            'asnafdarhalepeigiri',
            'asnafsodorgovahi',
            'asnafadamemkansodorgovahi',
            'bankbalaye7rooz',
            'bankpasokhdarmohlatmogharar',
            'bankpasokhbalaye7rooz',
            'asnafbalaye7rooz',
            'asnafpasokhdarmohlatmogharar',
            'asnafpasokhbalaye7rooz',
            ]


def get_newdatefor186():

    start_dates = [
        '13910101',
        '13960101',
        '13970101',
        '13970601',
        '13980101',
        '13980601',
        '13990101',
        '13990601',
        '14000101',
        '14000601',
        '14010101',
        '14010601',
    ]

    end_dates = [
        '13951229',
        '13961229',
        '13970531',
        '13971229',
        '13980531',
        '13981229',
        '13990531',
        '13991229',
        '14000531',
        '14001229',
        '14010531',
        '14011229',
    ]

    for date in zip(start_dates, end_dates):
        yield date


def return_sanim_download_links(p_instance, report_type, year):
    sanim_download_links = {
        'ezhar': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},ITXC,1,2",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},ITXB,1,2",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},VAT,1,2"
        ],
        'tashkhis_sader_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'tashkhis_eblagh_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'ghatee_sader_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'ghatee_eblagh_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
        ],
        'badvi_darjarian_dadrasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_DECISION_DEADLINE,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},3,1100,25,02"
        ],
        'badvi_darjarian_dadrasi_hamarz': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:453:{p_instance}:::453::"
        ],
        'amar_sodor_gharar_karshenasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:380:{p_instance}:::::"
        ],
        'amar_sodor_ray': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:482:{p_instance}:::::"
        ],
        'badvi_takmil_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_SORS_EBLAGH,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},9,1100,1,02"
        ],
        'tajdidnazer_darjarian_dadrasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_DECISION_DEADLINE,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},5,1100,25,03"
        ],
        'tajdidnazar_takmil_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_SORS_EBLAGH,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},11,1100,2,03"
        ],
    }

    return sanim_download_links[report_type]

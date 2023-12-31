# scores = {
#     'رسیدگی-حقیقی-مهم': 8,
#     'رسیدگی-حقیقی-بالا': 4,
#     'رسیدگی-حقیقی-متوسط': 2,
#     'رسیدگی-حقوقی-مهم': 10,
#     'رسیدگی-حقوقی-بالا': 6,
#     'رسیدگی-حقوقی-متوسط': 4,
#     'رسیدگی-مستغلات-مهم': 3,
#     'رسیدگی-مستغلات-بالا': 3,
#     'رسیدگی-مستغلات-متوسط': 3,
#     'رسیدگی-حقوق-مهم': 10,
#     'رسیدگی-حقوق-بالا': 6,
#     'رسیدگی-حقوق-متوسط': 4,
#     'رسیدگی-تکلیفی-مهم': 10,
#     'رسیدگی-تکلیفی-بالا': 6,
#     'رسیدگی-تکلیفی-متوسط': 4,
#     'رسیدگی-جریمه 169-مهم': 10,
#     'رسیدگی-جریمه 169-بالا': 6,
#     'رسیدگی-جریمه 169-متوسط': 4,
#     'رسیدگی-ارزش افزوده-مهم': 10,
#     'رسیدگی-ارزش افزوده-بالا': 6,
#     'رسیدگی-ارزش افزوده-متوسط': 4,


# }

def calculate_score(x, calc_type):
    if calc_type == 'roshd':
        if x > 20:
            return x * 10
        if x > 10 and x <= 20:
            return x * 8
        if x > 1 and x <= 10:
            return x * 5
        if x == 0:
            return x * 2
        else:
            return x * 1
    elif calc_type == 'tahaghogh':
        if x > 105:
            return x * 20
        if x > 99 and x <= 105:
            return x * 15
        if x >= 49 and x <= 99:
            return x * 10
        if x > 0 and x <= 49:
            return x * 5
        else:
            return x * 1

    elif calc_type == 'vazn':
        if x >= 15:
            return x * 10
        if x > 9 and x <= 14:
            return x * 8
        if x > 4 and x <= 9:
            return x * 5
        if x <= 4:
            return x * 2
        else:
            return x * 1


scores = {
    'ريسک مهم': {
        'رسیدگی': {
            'حقیقی': 8,
            'حقوقی': 10,
            'مستغلات': 3,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        'قطعی': {
            'حقیقی': 8,
            'حقوقی': 10,
            'مستغلات': 3,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        'ابلاغ': {
            'حقیقی': 8,
            'حقوقی': 10,
            'مستغلات': 3,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        '238': {
            'حقیقی': 10,
            'حقوقی': 10,
            'مستغلات': 10,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        'ارزش افزوده تبصره 6': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آلایندگی و پسماند': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه خالی': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ماده 77': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ارسال به اجرا': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آماده سازی جهت طرح در هیات': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه لوکس': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'تبصره 157': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        }
    },
    'ريسک بالا': {
        'رسیدگی': {
            'حقیقی': 4,
            'حقوقی': 6,
            'مستغلات': 3,
            'حقوق': 6,
            'تکلیفی': 6,
            '169': 6,
            'ارزش افزوده': 6,
        },
        'قطعی': {
            'حقیقی': 4,
            'حقوقی': 6,
            'مستغلات': 3,
            'حقوق': 6,
            'تکلیفی': 6,
            '169': 6,
            'ارزش افزوده': 6,
        },
        'ابلاغ': {
            'حقیقی': 4,
            'حقوقی': 6,
            'مستغلات': 3,
            'حقوق': 6,
            'تکلیفی': 6,
            '169': 6,
            'ارزش افزوده': 6,
        },
        '238': {
            'حقیقی': 10,
            'حقوقی': 10,
            'مستغلات': 10,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        'ارزش افزوده تبصره 6': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آلایندگی و پسماند': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه خالی': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ماده 77': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ارسال به اجرا': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آماده سازی جهت طرح در هیات': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه لوکس': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'تبصره 157': {
            'حقیقی': 0,
            'حقوقی': 0,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        }
    },
    'ريسک متوسط': {
        'رسیدگی': {
            'حقیقی': 2,
            'حقوقی': 4,
            'مستغلات': 3,
            'حقوق': 4,
            'تکلیفی': 4,
            '169': 4,
            'ارزش افزوده': 4,
        },
        'قطعی': {
            'حقیقی': 2,
            'حقوقی': 4,
            'مستغلات': 3,
            'حقوق': 4,
            'تکلیفی': 4,
            '169': 4,
            'ارزش افزوده': 4,
        },
        'ابلاغ': {
            'حقیقی': 2,
            'حقوقی': 4,
            'مستغلات': 3,
            'حقوق': 4,
            'تکلیفی': 4,
            '169': 4,
            'ارزش افزوده': 4,
        },
        '238': {
            'حقیقی': 10,
            'حقوقی': 10,
            'مستغلات': 10,
            'حقوق': 10,
            'تکلیفی': 10,
            '169': 10,
            'ارزش افزوده': 10,
        },
        'ارزش افزوده تبصره 6': {
            'حقیقی': 1,
            'حقوقی': 2,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آلایندگی و پسماند': {
            'حقیقی': 4,
            'حقوقی': 4,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه خالی': {
            'حقیقی': 1,
            'حقوقی': 1,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ماده 77': {
            'حقیقی': 2,
            'حقوقی': 2,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'ارسال به اجرا': {
            'حقیقی': 3,
            'حقوقی': 3,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'آماده سازی جهت طرح در هیات': {
            'حقیقی': 3,
            'حقوقی': 3,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'خانه لوکس': {
            'حقیقی': 2,
            'حقوقی': 2,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        },
        'تبصره 157': {
            'حقیقی': 1,
            'حقوقی': 1,
            'مستغلات': 0,
            'حقوق': 0,
            'تکلیفی': 0,
            '169': 0,
            'ارزش افزوده': 0,
        }
    },
}


# scores = {
#     'رسیدگی': {
#        'ریسک مهم': {
#             'حقیقی': 8,
#             'حقوقی': 10,
#             'مستغلات': 3,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#         'ريسک بالا': {
#             'حقیقی': 4,
#             'حقوقی': 6,
#             'مستغلات': 3,
#             'حقوق': 6,
#             'تکلیفی': 6,
#             '169': 6,
#             'ارزش افزوده': 6,
#         },
#        'ریسک متوسط': {
#             'حقیقی': 2,
#             'حقوقی': 4,
#             'مستغلات': 3,
#             'حقوق': 4,
#             'تکلیفی': 4,
#             '169': 4,
#             'ارزش افزوده': 4,
#         },
#     },
#     'قطعی': {
#        'ریسک مهم': {
#             'حقیقی': 8,
#             'حقوقی': 10,
#             'مستغلات': 3,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#         'ريسک بالا': {
#             'حقیقی': 4,
#             'حقوقی': 6,
#             'مستغلات': 3,
#             'حقوق': 6,
#             'تکلیفی': 6,
#             '169': 6,
#             'ارزش افزوده': 6,
#         },
#        'ریسک متوسط': {
#             'حقیقی': 2,
#             'حقوقی': 4,
#             'مستغلات': 3,
#             'حقوق': 4,
#             'تکلیفی': 4,
#             '169': 4,
#             'ارزش افزوده': 4,
#         },
#     },
#     'ابلاغ': {
#        'ریسک مهم': {
#             'حقیقی': 8,
#             'حقوقی': 10,
#             'مستغلات': 3,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#         'ريسک بالا': {
#             'حقیقی': 4,
#             'حقوقی': 6,
#             'مستغلات': 3,
#             'حقوق': 6,
#             'تکلیفی': 6,
#             '169': 6,
#             'ارزش افزوده': 6,
#         },
#        'ریسک متوسط': {
#             'حقیقی': 2,
#             'حقوقی': 4,
#             'مستغلات': 3,
#             'حقوق': 4,
#             'تکلیفی': 4,
#             '169': 4,
#             'ارزش افزوده': 4,
#         },
#     },
#     '238': {
#        'ریسک مهم': {
#             'حقیقی': 10,
#             'حقوقی': 10,
#             'مستغلات': 10,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#         'ريسک بالا': {
#             'حقیقی': 10,
#             'حقوقی': 10,
#             'مستغلات': 10,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#        'ریسک متوسط': {
#             'حقیقی': 10,
#             'حقوقی': 10,
#             'مستغلات': 10,
#             'حقوق': 10,
#             'تکلیفی': 10,
#             '169': 10,
#             'ارزش افزوده': 10,
#         },
#     },
#     'ارزش افزوده تبصره 6': {
#        'ریسک متوسط': {
#             'حقیقی': 1,
#             'حقوقی': 2,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'آلایندگی و پسماند': {
#        'ریسک متوسط': {
#             'حقیقی': 4,
#             'حقوقی': 4,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'خانه خالی': {
#        'ریسک متوسط': {
#             'حقیقی': 1,
#             'حقوقی': 1,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'ماده 77': {
#        'ریسک متوسط': {
#             'حقیقی': 2,
#             'حقوقی': 2,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'ارسال به اجرا': {
#        'ریسک متوسط': {
#             'حقیقی': 3,
#             'حقوقی': 3,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'آماده سازی جهت طرح در هیات': {
#        'ریسک متوسط': {
#             'حقیقی': 3,
#             'حقوقی': 3,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'خانه لوکس': {
#        'ریسک متوسط': {
#             'حقیقی': 2,
#             'حقوقی': 2,
#             'مستغلات': 0,
#             'حقوق': 0,
#             'تکلیفی': 0,
#             '169': 0,
#             'ارزش افزوده': 0,
#         },
#     },
#     'تبصره 157': {
#        'ریسک متوسط': {
#             'حقیقی': 1,
#             'حقوقی': 1,
#             'مستغلات': 1,
#             'حقوق': 1,
#             'تکلیفی': 1,
#             '169': 1,
#             'ارزش افزوده': 1,
#         },
#     },
# }

from elasticsearch import Elasticsearch, helpers
import requests, json, os, sys
from datetime import datetime, timedelta
from API2_check_and_get import API2
import pandas as pd
from pandas import DataFrame
import openpyxl
import time




es = Elasticsearch()
# es.indices.put_settings(index="post_index",
#                         body= {"index" : {
#                                 "max_result_window" : 500000
#                               }})
es2 = Elasticsearch("http://103.74.122.196:9200")


res = es.search(index="post_index", body={
    "size": 500000,
    "query": {
        "match_all":{}
    },
    '_source': [
        # "shortformDate", "message"
    ],
    # "sort": [
    #     {"shortformDate": {"order": "asc"}}
    # ]
})
date = []
for i in res["hits"]["hits"]:
    if i["_source"]["shortformDate"] not in date:
        date.append(i["_source"]["shortformDate"])
date = [datetime.strptime(ts, "%Y-%m-%d") for ts in date]
date.sort()
sorteddates = [datetime.strftime(ts, "%Y-%m-%d") for ts in date]
post_by_date = {}
for i in sorteddates:
    post_by_date[i] = []
for i in res["hits"]["hits"]:
    post_by_date[i["_source"]["shortformDate"]].append(i["_source"])
# print(post_by_date)


def create_field():
    # wb = openpyxl.load_workbook('demographic_behaviorKeywords.xlsx')
    # user_field = []
    # Sheet1 = wb['Sheet1']
    # len1 = 0
    # len2 = 0
    # len3 = 0
    # len4 = 0
    # len5 = 0
    # len6 = 0
    # lv3 = []
    # level1 = ''
    # level2 = ''
    # level3 = ''
    # level4 = ''
    # level5 = ''
    # level6 = ''
    # for i in range(3, 1215):
    #     cellNameA = f'A{i}'
    #     cellNameB = f'B{i}'
    #     cellNameC = f'C{i}'
    #     cellNameD = f'D{i}'
    #     cellNameE = f'E{i}'
    #     cellNameF = f'F{i}'
    #     cellNameG = f'G{i}'
    #
    #     cellDataA = Sheet1[cellNameA].value
    #     cellDataB = Sheet1[cellNameB].value
    #     cellDataC = Sheet1[cellNameC].value
    #     cellDataD = Sheet1[cellNameD].value
    #     cellDataE = Sheet1[cellNameE].value
    #     cellDataF = Sheet1[cellNameF].value
    #     cellDataG = Sheet1[cellNameG].value
    #     if cellDataA == 'BEHAVIOR INDICATORS':
    #         continue
    #     if cellDataB is None and cellDataC is None and cellDataD is None and cellDataE is None and cellDataF is None and cellDataG is None:
    #         continue
    #     field = ''
    #     if cellDataB is not None:
    #
    #         level1 = cellDataB
    #         if cellDataC is None:
    #             field = level1
    #             len1 += 1
    #
    #     if cellDataC is not None:
    #
    #         level2 = cellDataC
    #         if cellDataD is None:
    #             field = level1 + "." + level2
    #             len2 += 1
    #
    #     if cellDataD is not None:
    #
    #         level3 = cellDataD
    #         if cellDataE is None:
    #             field = level1 + "." + level2 + "." + level3
    #             len3 += 1
    #     if cellDataE is not None:
    #
    #         level4 = cellDataE
    #         if cellDataF is None:
    #             field = level1 + "." + level2 + "." + level3 + "." + level4
    #             len4 += 1
    #
    #     if cellDataF is not None:
    #
    #         level5 = cellDataF
    #         if cellDataG is None:
    #             field = level1 + "." + level2 + "." + level3 + "." + level4 + "." + level5
    #             len5 += 1
    #         else:
    #             field += "."
    #     if cellDataG is not None:
    #         len6 += 1
    #         level6 = cellDataG
    #         field = level1 + "." + level2 + "." + level3 + "." + level4 + "." + level5 + "." + level6
    #
    #     user_field.append(field)
    # # print(len1, len2, len3, len4, len5, len6)
    # # print(lv3)
    user_field = ['phone', 'tuổi', 'giới tính', 'tình trạng hôn nhân', 'gia đình', 'Mức thu nhập', 'Mức sống',
                  'nghề nghiệp', 'trình độ học vấn', 'ngôn ngữ',
                  'quê quán', 'nơi ở hiện tại', 'sở hữu bất động sản', 'sở hữu xe cộ', 'ngân hàng', 'công ty bảo hiểm',
                  'bảo hiểm', 'vay', 'ngân hàng cho vay', 'công ty tài chính cho vay', 'thẻ/ví', 'hãng thẻ',
                  'tiết kiệm', 'tiết kiệm tại các ngân hàng', 'đầu tư', 'sức khỏe',
                  'giáo dục', 'du lịch', 'du học', 'sở thích']
    return user_field


def get_infor_user_by_date(userId, post_in_date):
    res_user = es.search(index="user_cogroup_index", body={
        "query": {
            "match_phrase": {
                "id": userId
            }
        },
    })
    infor = res_user["hits"]["hits"][0]['_source']
    post_user_by_date = []
    for post in post_in_date:
        if post["sourceId"] == userId:
            post_user_by_date.append(post)

    user = {
        "infor": infor,
        "infor_post": post_user_by_date
    }
    return user

def export_file_by_date(date):
    df_get = DataFrame({'field': create_field()})
    user_by_date = 0
    post_in_date = post_by_date[date]
    user_ids = []
    for post in post_in_date:
        if post["sourceId"] not in user_ids:
            user_ids.append(post["sourceId"])
    list_dict_by_date = []
    for user_id in user_ids:
        start_time = time.time()
        user_data = get_infor_user_by_date(user_id, post_in_date)

        dictObj, id = API2().get_all(user_id, user_data)
        user_result = []
        # for key1, value1 in dictObj.items():
        #     # print(key1)
        #     # dict1 = len(value1)
        #     # print(value1)
        #
        #     if type(value1) == dict:
        #         # print(value1)
        #         for key2, value2 in value1.items():
        #
        #             if type(value2) == dict:
        #                 # print(value2 == '1')
        #                 for key3, value3 in value2.items():
        #                     if type(value3) == dict:
        #                         for key4, value4 in value3.items():
        #
        #
        #                             if type(value4) == dict:
        #                                 for key5, value5 in value4.items():
        #
        #
        #                                     if type(value5) == dict:
        #                                         for key6, value6 in value5.items():
        #                                             user_result.append(value6)
        #                                     else:
        #                                         user_result.append(value5)
        #                             else:
        #                                 user_result.append(value4)
        #                     else:
        #                         user_result.append(value3)
        #             else:
        #                 user_result.append(value2)
        #     else:
        #         # print(value1 == '')
        #         user_result.append(value1)
        # # print(k3)
        # try:
        #     user_result.append(str(user_data['infor']['phone']))
        # except:
        #     user_result.append('')
        # check_tuoi = False
        # for key, value in dictObj['Tuổi'].items():
        #     if value != '':
        #         check_tuoi = True
        #         user_result.append(key)
        # if not check_tuoi:
        #     user_result.append('')
        # for key, value in dictObj['Giới tính'].items():
        #     if value != '':
        #         user_result.append(key)
        # check_honnhan = False
        # for key, value in dictObj['Tình trạng hôn nhân'].items():
        #     if value != '':
        #         check_honnhan = True
        #         user_result.append(key)
        # if not check_honnhan:
        #     user_result.append('')
        # if dictObj['Gia đình']['Đã có con hay chưa'] != '':
        #     user_result.append('Đã có con')
        # else:
        #     user_result.append('')
        # user_result.append('')
        # user_result.append('')
        # check_nghenghiep = False
        # nghenghiep_list = []
        # for key, value in dictObj['Nghề nghiệp'].items():
        #     if value != '':
        #         check_nghenghiep = True
        #         nghenghiep_list.append(key)
        # if not check_nghenghiep:
        #     user_result.append('')
        # else:
        #     user_result.append(', '.join(nghenghiep_list))
        # check_hocvan = False
        # hocvan_list = []
        # for key, value in dictObj['trình độ học vấn'].items():
        #     if value != '':
        #         check_hocvan = True
        #         hocvan_list.append(key)
        # if not check_hocvan:
        #     user_result.append('')
        # else:
        #     user_result.append(', '.join(hocvan_list))
        # check_ngonngu = False
        # ngonngu_list = []
        # for key, value in dictObj['Ngôn ngữ'].items():
        #     if value != '':
        #         check_ngonngu = True
        #         ngonngu_list.append(key)
        # if not check_ngonngu:
        #     user_result.append('')
        # else:
        #     user_result.append(', '.join(ngonngu_list))
        # check_quequan = False
        # for key, value in dictObj['Quê quán'].items():
        #     if value != '':
        #         check_quequan = True
        #         user_result.append(key)
        # if not check_quequan:
        #     user_result.append('')
        # check_noio = False
        # for key, value in dictObj['Nơi ở hiện tại'].items():
        #     if value != '':
        #         check_noio = True
        #         user_result.append(key)
        # if not check_noio:
        #     user_result.append('')
        #
        # check_sohuubds = False
        # bds_list = []
        # for key, value in dictObj['Sở hữu']['Bất động sản'].items():
        #     if type(value) != dict:
        #         if value != '':
        #             check_sohuubds = True
        #             bds_list.append(key)
        #     else:
        #         for key2, value2 in value.items():
        #             if value2 != '':
        #                 check_sohuubds = True
        #                 bds_list.append('BĐS nổi bật')
        #                 break
        # if not check_sohuubds:
        #     user_result.append('')
        # else:
        #     a = set(bds_list)
        #     user_result.append(', '.join(a))
        #
        # check_sohuuxe = False
        # sohuuxe_list = []
        # for key, value in dictObj['Sở hữu']['Xe cộ'].items():
        #     if value != '':
        #         check_sohuuxe = True
        #         sohuuxe_list.append(key)
        # if not check_sohuuxe:
        #     user_result.append('')
        # else:
        #     a = set(sohuuxe_list)
        #     user_result.append(', '.join(a))

        check_nganhang = False
        nganhang_list = []
        for key, value in dictObj['Ngân hàng'].items():
            if value != '':
                check_nganhang = True
                nganhang_list.append(key)
        if not check_nganhang:
            user_result.append('')
        else:
            a = set(nganhang_list)
            user_result.append(', '.join(a))

        check_congtybaohiem = False
        congtybaohiem_list = []
        for key, value in dictObj['Bảo hiểm']['loại hình bảo hiểm'][
            'Chị lấy ds mấy công ty bảo hiểm nghe có vẻ nổi tiếng nhất + list 18 cty bảo hiểm có vốn điều lệ lớn nhất'].items():
            if value != '':
                check_congtybaohiem = True
                congtybaohiem_list.append(key)
        if not check_congtybaohiem:
            user_result.append('')
        else:
            a = set(congtybaohiem_list)
            user_result.append(', '.join(a))

        check_loaibaohiem = False
        loaibaohiem_list = []
        for key, value in dictObj['Bảo hiểm']['loại hình bảo hiểm']['bảo hiểm thương mại'].items():
            if type(value) != dict:
                if value != '':
                    check_loaibaohiem = True
                    loaibaohiem_list.append(key)
            else:
                for key2, value2 in value.items():
                    if type(value2) != dict:
                        if value2 != '':
                            check_loaibaohiem = True
                            loaibaohiem_list.append(key2)
                    else:
                        for key3, value3 in value2.items():
                            if value3 != '':
                                check_loaibaohiem = True
                                loaibaohiem_list.append(key3)

        if not check_loaibaohiem:
            user_result.append('')
        else:
            a = set(loaibaohiem_list)
            user_result.append(', '.join(a))

        check_vay = False
        vay_list = []
        for key, value in dictObj['vay'].items():
            if type(value) != dict:
                if value != '':
                    check_vay = True
                    vay_list.append(key)
            else:
                if key != 'vay ngân hàng' or key != 'công ty tài chính (cho vay)':
                    for key2, value2 in value.items():
                        if value2 != '':
                            check_vay = True
                            vay_list.append(key)
                            break
        if not check_vay:
            user_result.append('')
        else:
            a = set(vay_list)
            user_result.append(', '.join(a))

        check_vaynganhang = False
        vaynganhang_list = []
        for key, value in dictObj['vay']['vay ngân hàng'].items():
            if value != '':
                check_vaynganhang = True
                vaynganhang_list.append(key)
        if not check_vaynganhang:
            user_result.append('')
        else:
            a = set(vaynganhang_list)
            user_result.append(', '.join(a))

        check_congtychovay = False
        congtychovay_list = []
        for key, value in dictObj['vay']['công ty tài chính (cho vay)'].items():
            if value != '':
                check_congtychovay = True
                congtychovay_list.append(key)
        if not check_congtychovay:
            user_result.append('')
        else:
            a = set(congtychovay_list)
            user_result.append(', '.join(a))

        check_the = False
        the_list = []
        for key, value in dictObj['thẻ/ví'].items():
            if type(value) != dict:
                if value != '':
                    check_the = True
                    the_list.append(key)
            else:
                if key != 'mở thẻ ngân hàng' and key != 'tiết kiệm':
                    for key2, value2 in value.items():
                        if value2 != '':
                            check_the = True
                            the_list.append(key2)
        if not check_the:
            user_result.append('')
        else:
            a = set(the_list)
            user_result.append(', '.join(a))

        check_hangthe = False
        hangthe_list = []
        for key, value in dictObj['thẻ/ví']['mở thẻ ngân hàng'].items():
            if value != '':
                check_hangthe = True
                hangthe_list.append(key)
        if not check_hangthe:
            user_result.append('')
        else:
            a = set(hangthe_list)
            user_result.append(', '.join(a))

        check_tietkiem = False
        tietkiem_list = []
        for key, value in dictObj['thẻ/ví']['tiết kiệm'].items():
            if type(value) != dict:
                if value != '':
                    check_tietkiem = True
                    tietkiem_list.append(key)
            else:
                if key != 'tiết kiệm ngân hàng':
                    for key2, value2 in value.items():
                        if value2 != '':
                            check_tietkiem = True
                            tietkiem_list.append(key)
        if not check_tietkiem:
            user_result.append('')
        else:
            a = set(tietkiem_list)
            user_result.append(', '.join(a))

        check_tietkiemnganhang = False
        tietkiemnganhang_list = []
        for key, value in dictObj['thẻ/ví']['tiết kiệm']['tiết kiệm ngân hàng'].items():
            if value != '':
                check_tietkiemnganhang = True
                tietkiemnganhang_list.append(key)
        if not check_tietkiemnganhang:
            user_result.append('')
        else:
            a = set(tietkiemnganhang_list)

            user_result.append(', '.join(a))

        check_dautu = False
        dautu_list = []
        for key, value in dictObj['đầu tư'].items():
            if type(value) != dict:
                if value != '':
                    check_dautu = True
                    dautu_list.append(key)
            else:
                for key2, value2 in value.items():
                    if type(value2) != dict:
                        if value2 != '':
                            check_dautu = True
                            dautu_list.append(key)
                            break
                    else:
                        for key3, value3 in value2.items():
                            if value3 != '':
                                check_dautu = True
                                dautu_list.append(key)
                                break
        if not check_dautu:
            user_result.append('')
        else:
            a = set(dautu_list)
            user_result.append(', '.join(a))

        check_suckhoe = False
        suckhoe_list = []
        for key, value in dictObj['sức khỏe'].items():
            if type(value) != dict:
                if value != '':
                    check_suckhoe = True
                    suckhoe_list.append(key)
            else:
                for key2, value2 in value.items():
                    if type(value2) != dict:
                        if value2 != '':
                            check_suckhoe = True
                            suckhoe_list.append(key)
                            break
                    else:
                        for key3, value3 in value2.items():
                            if value3 != '':
                                check_suckhoe = True
                                suckhoe_list.append(key)
                                break
        if not check_suckhoe:
            user_result.append('')
        else:
            a = set(suckhoe_list)
            user_result.append(', '.join(a))

        check_giaoduc = False
        giaoduc_list = []
        for key, value in dictObj['Giáo dục'].items():
            if type(value) != dict:
                if value != '':
                    check_giaoduc = True
                    giaoduc_list.append(key)
            else:
                for key2, value2 in value.items():
                    if value2 != '':
                        check_giaoduc = True
                        giaoduc_list.append(key)
                        break
        if not check_giaoduc:
            user_result.append('')
        else:
            a = set(giaoduc_list)
            user_result.append(', '.join(a))

        check_dulich = False
        dulich_list = []
        for key, value in dictObj['Du lịch'].items():
            for key2, value2 in value.items():
                for key3, value3 in value2.items():
                    if type(value3) != dict:
                        if value3 != '':
                            check_dulich = True
                            dulich_list.append(key)
                            break
                    else:
                        for key4, value4 in value3.items():
                            if value4 != '':
                                check_dulich = True
                                dulich_list.append(key)
                                break
        if not check_dulich:
            user_result.append('')
        else:
            a = set(dulich_list)
            user_result.append(', '.join(a))

        check_duhoc = False
        duhoc_list = []
        for key, value in dictObj['du học']['đất nước'].items():
            if value != '':
                check_duhoc = True
                duhoc_list.append(key)
        if not check_duhoc:
            user_result.append('')
        else:
            a = set(duhoc_list)
            user_result.append(', '.join(a))

        check_sothich = False
        sothich_list = []
        for key, value in dictObj['sở thích'].items():
            for key2, value2 in value.items():
                if type(value2) != dict:
                    if value2 != '':
                        check_sothich = True

                        sothich_list.append(key)
                        break
                else:
                    for key3, value3 in value2.items():
                        if type(value3) != dict:
                            if value3 != '':
                                check_sothich = True

                                sothich_list.append(key)
                                break
                        else:
                            for key4, value4 in value3.items():
                                if type(value4) != dict:
                                    if value4 != '':
                                        check_sothich = True

                                        sothich_list.append(key)
                                        break
                                else:
                                    for key5, value5 in value4.items():
                                        if value5 != '':
                                            check_sothich = True

                                            sothich_list.append(key)
                                            break
        if not check_sothich:
            user_result.append('')
        else:
            a = set(sothich_list)
            user_result.append(', '.join(a))
        user_by_date += 1
        print(user_by_date)
        # print(is_xeco/API1.user.count)
        # print(user_result)
        # print(dictObj)
        dict_field = {'user_id': id, 'date': date, 'field': user_result}
        list_dict_by_date.append(dict_field)
    return list_dict_by_date

    #     df_get['( facebook.com/' + id + ' )'] = user_result
    #     run_time_user = time.time() - start_time
    #     print(run_time_user)
    # df_get.to_excel(r'C:\Users\acer\Documents\hoc tap\thuc tap bao hiem\predict 1000 truong\predict from other source\1000 truong post\{}.xlsx'.format(date), encoding='utf-8')
total_data_field = []
for date in sorteddates:
    total_data_field += export_file_by_date(date)
    print(date)
with open('get_json_post_field.json', 'w', encoding='utf-8') as f:
    json.dump(total_data_field, f, ensure_ascii=False, indent=4)



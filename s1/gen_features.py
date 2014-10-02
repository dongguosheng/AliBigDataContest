#!/usr/bin/python
# -*- coding: utf-8 -*-

import tmall as tm
from datetime import date
import sys

def save_grades(grade_dict, filename, time_range_list, real_dict, flag):
    '''
    Save train or test grades to file. (f1,f2,f3,...f12,y,u_id,brand_id)
                                    or (f1,f2,f3,...f48,y,u_id,brand_id)
    '''
    with open(filename, 'w') as f:
        for key_pair, grade_tuple_list in grade_dict.items():
            line = ''
            for time_range in time_range_list:
                temp_list = [grade_tuple for grade_tuple in grade_tuple_list if grade_tuple[0] == time_range]
                if len(temp_list) == 1:
                    line += to_str(temp_list[0][1]) + ','
                else:
                    line += '0,'    # 12 dims
                    # line += '0,0,0,0,' # 48 dims
            if flag == 'last':
                f.write(line + '0,')
            elif flag == 'front':
                print 'Hit'
                f.write(line + isHit(key_pair, real_dict) + ',')
            f.write(key_pair[0] + ',' + key_pair[1] + '\n')

def to_str(grade):
    if isinstance(grade, (float, list)):
        if isinstance(grade, float):
            return str(grade)
        else:
            return ','.join(str(e) for e in grade)
    else:
        print 'Wrong grades format.'

def isHit(key_pair, real_dict):
    if key_pair[0] in real_dict:
        # print real_dict[key_pair[0]]
        if key_pair[1] in real_dict[key_pair[0]]:
            # print key_pair[1]
            return '1'
        return '0'
    else:
        return '0'

def main():
    if len(sys.argv) != 5:
        print 'Usage:\t./gen_features.py ./dataset/three_month_with_date.data grades_3.txt front ./dataset/real_result_larger_1.txt\nor\
        ./gen_features.py ./dataset/last_three_month.txt grades_3_last.txt last ./dataset/real_result_larger_1.txt'
    else:
        filename = sys.argv[1]
        # filename = './dataset/last_three_month.txt'
        file_ = sys.argv[2]
        # file_ = 'grades_3_last.txt'
        data_dict = tm.load_from_initial_data(filename)
        real_file = sys.argv[4]

        rs = {}
        if sys.argv[3] == 'front':
            rs = tm.get_time_dict(data_dict, date(2014, 7, 15), date(2014, 4, 14))
        elif sys.argv[3] == 'last':
            rs = rs = tm.get_time_dict(data_dict, date(2014, 8, 15), date(2014, 5, 15))
        else:
            print 'Not front or last!'

        time_range_list = [(0, 7), (0, 16), (0, 23), (0, 30), 
                       (0, 37), (0, 46), (0, 53), (0, 60), 
                       (0, 67), (0, 76), (0, 83), (0, 92)]
        # time_range_list = [(0, 7), (7, 16), (16, 23), (23, 30), 
        #                    (30, 37), (37, 46), (46, 53), (53, 60), 
        #                    (60, 67), (67, 76), (76, 83), (83, 92)]
        grade_dict = tm.cal_grade_dict(rs, time_range_list, tm.cal_grade_1444)

        # load real result
        real_dict = tm.load_result_list(real_file)

        save_grades(grade_dict, file_, time_range_list, real_dict, sys.argv[3])

if __name__ == '__main__':
    main()

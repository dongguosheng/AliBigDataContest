#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import tmall as tm

def mtet(grade, threshold):
    '''
    More than equal to threshold.
    '''
    if grade[1] == 0 and grade[2] == 0 and grade[3] == 0 and grade[0] < threshold:
        return False
    else:
        return True

def main():
    # tm.gen_grade_file(tm.load_merged_actions('./dataset_3_1/three_months.txt'), './dataset_3_1/grades_3_months.txt')

    if len(sys.argv) != 3:
        print 'Usage:\tgen_real_result.py last_month.txt real_result.txt'
    else:
        threshold = 4
        rs_dict = tm.load_merged_actions(sys.argv[1])
        rs = {}
        for key_pair, action_list in rs_dict.items():
            grade = tm.cal_grade(action_list)
            if isinstance(grade, float) and grade >= threshold:
                if key_pair[0] not in rs:
                    rs[key_pair[0]] = []
                rs[key_pair[0]].append(key_pair[1])
                # f.write(key_pair[0] + '\t' + key_pair[1] + '\n')
            elif isinstance(grade, list) and mtet(grade, threshold):
                if key_pair[0] not in rs:
                    rs[key_pair[0]] = []
                rs[key_pair[0]].append(key_pair[1])
                # f.write(key_pair[0] + '\t' + key_pair[1] + '\n')
            else:
                pass
                # print 'Wrong grades format!'
        with open(sys.argv[2], 'w') as f:
            for u_id, brand_id_list in rs.items():
                f.write(u_id + '\t' + ','.join(brand_id_list) + '\n')




if __name__ == '__main__':
    main() 

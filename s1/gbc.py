#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
import tmall as tm
import sys

def gbc(train='grades_3_sample_ratio_1.txt', test='grades_3_last.txt', lr_result = 'gbc_result_1.txt'):
    train_array = np.loadtxt(train, delimiter=',')
    X = train_array[:, :-3]
    X = np.divide(X - X.min(axis=0), X.max(axis=0) - X.min(axis=0))
    # print X
    Y = train_array[:, -3]
    # print Y
    gbc = GradientBoostingClassifier()
    gbc.fit(X, Y)
    data_predict = np.loadtxt(test, delimiter=',')
    X_predict = data_predict[:, :-3]
    X_predict = np.divide(X_predict - X_predict.min(axis=0), X_predict.max(axis=0) - X_predict.min(axis=0))
    rs = gbc.predict(X_predict)
    rs_proba = gbc.predict_proba(X_predict)
    # print np.append(np.array([rs]).transpose(), rs_proba[:, -1:], axis=1)
    rs = np.append(np.append(np.array([rs]).transpose(), rs_proba[:, -1:], axis=1), data_predict[:, -2:], axis=1)
    # print rs
    np.savetxt(lr_result, rs, fmt='%f', delimiter='\t')

def gen_result(lr_result, grades_3_test, final_result):
    # TODO
    lines_lr = open(lr_result).readlines()
    lines_key_pair = open(grades_3_test).readlines()
    rs_list = []
    if len(lines_lr) != len(lines_key_pair):
        print 'Wrong input file!'
    else:
        for i in range(len(lines_lr)):
            if float(lines_lr[i]) != 0:
                rs_list.append(lines_key_pair[i].split(',')[-2] + '\t' + lines_key_pair[i].split(',')[-1])
    rs = {}
    for line in rs_list:
        temp_list = line.split()
        if not temp_list[0] in rs:
            rs[temp_list[0]] = []
        if not temp_list[1] in rs[temp_list[0]]:
            rs[temp_list[0]].append(temp_list[1])
    tm.save_with_format(rs, final_result)

def main():
    if len(sys.argv) != 3:
        print 'Usage:\t./gbr.py grades_3_sample_ratio_1.txt grades_3_test.txt'
    else:
        gbc(train=sys.argv[1], test=sys.argv[2])
        # show rec numbers
        f_temp = open('temp', 'w')
        rec_num = 0
        with open('gbc_result_1.txt') as f:
            for line in f:
                if int(line.split()[0][0]) == 1:
                    tmp_list = line.split()
                    f_temp.write(str(int(float(tmp_list[2]))) + '\t' + str(int(float(tmp_list[3]))) + '\t' + tmp_list[1] + '\n')
                    rec_num += 1
        print 'Rec numbers: ' + str(rec_num)
            
        # gen_result('lr_result_1.txt', sys.argv[2], 'rec_result.txt')

if __name__ == '__main__':
    main()

#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import operator
import sys
import numpy as np
import tmall as tm

def traverse_dir(directory, func):
    file_list = os.listdir(directory)
    # print file_list
    rs = {}
    for file_ in file_list:
        print directory + file_
        rs = func(directory + file_, rs)

    for key_pair, proba_tuple in rs.items():
        # if proba_tuple[1] != 50:
        #     print proba_tuple[1]
        rs[key_pair] = proba_tuple[0] / proba_tuple[1] # lr

    sorted_rs = sorted(rs.iteritems(), key=operator.itemgetter(1), reverse=True)
    return sorted_rs

def load_proba_dict(file_, rs_dict):
    with open(file_) as f:
        for line in f:
            u_id = int(float(line.split()[0]))
            brand_id = int(float(line.split()[1]))
            proba = float(line.split()[2])
            if (u_id, brand_id) not in rs_dict:
                rs_dict[(u_id, brand_id)] = (proba, 1)
            else:
                rs_dict[(u_id, brand_id)] = (proba + rs_dict[(u_id, brand_id)][0], rs_dict[(u_id, brand_id)][1] + 1)
    print len(rs_dict)
    return rs_dict

def main():
    if len(sys.argv) != 4:
        print 'Usage:\t./traverseDir.py ./directory/ out.txt 3500'
    else:
        rs = traverse_dir(sys.argv[1], load_proba_dict)
        with open(sys.argv[2], 'w') as f:
            print 'After merged: ' + str(len(rs))
            for pair in rs[: int(sys.argv[3])]:
                f.write(str(pair[0][0]) + '\t' + str(pair[0][1]) + '\t' + str(pair[1]) + '\n')
        tm.gen_final_result(sys.argv[2], 'rec_result.txt')


if __name__ == '__main__':
    main()

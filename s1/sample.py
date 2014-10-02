#!/usr/bin/python
# -*- coding: utf-8 -*-

import random
import sys

def sample(source_grades_file, ratio):
    '''
    Sampling from the source grades file, positive : negetive = 1 : ratio.
    '''
    ratio = float(ratio)

    negetive_list = []
    positive_list = []

    with open(source_grades_file) as f_grades:
        for grades_line in f_grades:
            if int(grades_line.split(',')[-3]) == 1: 
                positive_list.append(grades_line)
            else:
                negetive_list.append(grades_line)

    for i in range(20):
        random.shuffle(negetive_list)
        negetive_sample_list = negetive_list[: int(len(positive_list) * ratio)]
        print 'Sample ' + str(len(positive_list) * (1 + ratio)) + ' from ' + str(len(negetive_list) + len(positive_list))
        save_sample_result(source_grades_file[:-4] + '_sample_ratio_' + str(ratio) + '_' + str(i) + '.txt', positive_list, negetive_sample_list)

def save_sample_result(filename, positive_list, negetive_sample_list):
    with open('./sample/' + filename, 'w') as f_features_sample:
        for positive in positive_list:
            f_features_sample.write(positive)
        for negetive in negetive_sample_list:
            f_features_sample.write(negetive)

def main():

    if len(sys.argv) != 3:
        print 'Usage:\t./sample.py grade_3_train.txt 1'
    else:
        sample(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
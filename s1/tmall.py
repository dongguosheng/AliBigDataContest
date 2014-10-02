#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
	Some utils functions by guosheng.
'''

import math
import sys
from datetime import date, timedelta
import random

def cal_grade_(action_list):
	'''
	cal_grade(action_list)
		action_list => list of ints
		return value => grade of double

	Calculate a grade according the actions in the action list.
		Use int to represent an action:
			0 => 点击 (click)
			1 => 购买 (buy)
			2 => 收藏 (favorite)
			3 => 购物车 (cart)
			         | 1.0 (buy only)
			grades = | 1.0 (favorite and cart)
					 | 0.8 (favorite only)
			         | 0.8 (cart only)
			         | 0.8 * | 1 (frequency > 9)
			                 | log10(frequency + 1) (frequency <= 9) (click only)
			         | 0.1 * | 1 (frequency > 9)                     | + 
			                 | log10(frequency + 1) (frequency <= 9) |   0.8 (click and favorite/cart)
	'''
	if 1 in action_list:
		return 1.0
	elif (2 in action_list) and (3 in action_list):
		return 1.0
	elif (2 in action_list) and (not 0 in action_list):
		return 0.8
	elif (3 in action_list) and (not 0 in action_list):
		return 0.8
	elif (0 in action_list) and (not 2 in action_list) and (not 3 in action_list):
		# print action_list
		frequency = len([action for action in action_list if action == 0])
		# print "frequency: ", frequency
		return 0.8 * (1 if frequency > 9 else math.log10(frequency + 1))
		# return 0.0 # filter click only
	else:
		frequency = len([action for action in action_list if action == 0])
		return 0.1 * (1 if frequency > 9 else math.log10(frequency + 1)) + 0.8

def cal_grade_1444(action_list):
	'''
	Calculate grade use 1 4 4 4.
	'''
	grade = 0.0
	for action in action_list:
		if action == 0:
			grade += 1.0
		elif action == 1:
			grade += 4.0
		elif action == 2:
			grade += 4.0
		else:
			grade += 4.0
	return grade

def cal_grade_1544(action_list):
	'''
	Calculate grade use 1 5 4 4.
	'''
	grade = 0.0
	for action in action_list:
		if action == 0:
			grade += 1.0
		elif action == 1:
			grade += 5.0
		elif action == 2:
			grade += 4.0
		else:
			grade += 4.0
	return grade

def cal_grade_1555(action_list):
	'''
	Calculate grade use 1 5 5 5.
	'''
	grade = 0.0
	for action in action_list:
		if action == 0:
			grade += 1.0
		elif action == 1:
			grade += 5.0
		elif action == 2:
			grade += 5.0
		else:
			grade += 5.0
	return grade

def cal_grade(action_list):
	'''
	Calculate grade list [num of 0, num of 1, num of 2, num of 3]
	'''
	grade_list = [0.0, 0.0, 0.0, 0.0]
	for i in range(4):
		grade_list[i] = float(len([action for action in action_list if action == i]))
	return grade_list

def to_date(date_str):
	'''
	date_str => 2014-04-01
	'''
	temp_list = date_str.split('-')
	return date(int(temp_list[0]), int(temp_list[1]), int(temp_list[2]))

def load_merged_actions(filename):
	'''
	load_merged_actions(filename)
		filename => initial data file with format (u_id	brand_id	action)
		return value => dict
							key => (u_id, brand_id)
							value => list of actions

	Read the initial data line by line, store in a dict.
	'''
	rs_dict = {}
	with open(filename) as f:
		for line in f:
			temp_list = line.split()
			u_id = temp_list[0]
			brand_id = temp_list[1]
			action = int(temp_list[2])

			if (u_id, brand_id) not in rs_dict:
				rs_dict[(u_id, brand_id)] = []
			rs_dict[(u_id, brand_id)].append(action)
			
	return rs_dict

def load_from_initial_data(filename):
	'''
	load_from_initial_data(filename)
		filename => initial data file with format (u_id	brand_id	action	date)
		return value => dict
							key => u_id
							value => dict (key => brand_id 
										   value => [(action, date), ...])

	Read the initial data line by line, store in a dict.
	'''
	rs_dict = {}
	with open(filename) as f:
		for line in f:
			temp_list = line.split()
			u_id = temp_list[0]
			brand_id = temp_list[1]
			action = int(temp_list[2])
			date_str = temp_list[3]

			if u_id not in rs_dict:
				rs_dict[u_id] = {}

				if brand_id not in rs_dict[u_id]:
					rs_dict[u_id][brand_id] = []

				rs_dict[u_id][brand_id].append((action, to_date(date_str)))
					
			else:
				if brand_id not in rs_dict[u_id]:
					rs_dict[u_id][brand_id] = []

				rs_dict[u_id][brand_id].append((action, to_date(date_str)))
	return rs_dict

def split_dict(rs_dict):
	'''
	Random split dict into 2 dicts. Return (train_dict, test_dict).
	train_dict and test_dict's structure:
		key => u_id
		value => dict (key => brand_id; value => [(action, date),...])
	'''
	u_id_list = [key for key in rs_dict]
	random.shuffle(u_id_list)
	train_dict = {}
	test_dict = {}
	for u_id in u_id_list:
		if len(train_dict) < len(rs_dict) / 2:
			train_dict[u_id] = rs_dict[u_id]
		else:
			test_dict[u_id] = rs_dict[u_id]

	print 'train_dict length:', len(train_dict)
	print 'test_dict length:', len(test_dict)
	return (train_dict, test_dict)

def gen_mapping(last_day, first_day):
	'''
	Result a dict (key => date; value => number of 0 - 90).
	'''
	rs = {}
	for i in range((last_day - first_day).days + 1):
		rs[last_day - timedelta(days=i)] = i
	return rs

def get_time_dict(rs_dict, last_day, first_day):
	'''
	Get dict, key => number of 0 - (last_day - first_day)
			  value => dict (key => (u_id, brand_id); value => action list)
	'''
	rs = {}
	print last_day - first_day
	for i in range((last_day - first_day).days + 1):
		rs[i] = {}

	time_to_num_dict = gen_mapping(last_day, first_day)

	for u_id, brand_id_dict in rs_dict.items():
		for brand_id, action_list_with_time in brand_id_dict.items():
			for action_with_time in action_list_with_time:
				if (u_id, brand_id) not in rs[time_to_num_dict[action_with_time[1]]]:
					rs[time_to_num_dict[action_with_time[1]]][(u_id, brand_id)] = [action_with_time[0]]
				else:
					rs[time_to_num_dict[action_with_time[1]]][(u_id, brand_id)].append(action_with_time[0])
	
	return rs

def merge_time_range(time_dict, days, start=0, func=cal_grade_1444):
	'''
	Start 0 => 2014-07-15
	End 91 => 2014-04-15
	Return rs_dict (key => tuple of uid and brand_id; value => (time_range, grade)) or (time_range, [grade_list])
	'''
	rs_dict = {}
	for day in range(start, days):
		for key_pair, action_list in time_dict[day].items():
			if key_pair not in rs_dict:
				rs_dict[key_pair] = []
			rs_dict[key_pair].extend(action_list)

	for key_pair, action_list in rs_dict.items():
		rs_dict[key_pair] = ((start, start + days), func(action_list))

	return rs_dict

def load_uid_brand_id_tuple(initial_file):
	'''
	Load a dict (key => tuple (u_id, brand_id); value => [])
	'''
	return [key_pair for key_pair in load_merged_actions(initial_file)]


def cal_grade_dict(time_dict, time_range_list, func=cal_grade_1444):
	'''
	time_range_list => list of tuple (start, end)
	Return a dict (key => (u_id, brand_id); value => list of tuple (time range, grade))
	'''
	# get all the (u_id, brand_id) pair
	rs_dict = {}
	for time, value in time_dict.items():
		for key_pair, action_list in value.items():
			rs_dict[key_pair] = []

	for time_range in time_range_list:
		time_merged_dict = merge_time_range(time_dict, time_range[1], start=time_range[0], func=func)
		for key_pair, grade_tuple in time_merged_dict.items():
			# print key_pair, grade_tuple
			rs_dict[key_pair].append(grade_tuple)

	return rs_dict


def gen_grade_file(rs_dict, filename):
	'''
	gen_grade_file(rs_dict, filename)
		rs_dict => return value of load_merged_actions
		filename => grade file that used for recommendation

	Save the grades to file.
		u_id brand_id	0.86741345
	'''
	with open(filename, 'w') as f:
		for key_pair, action_list in rs_dict.items():
			# print key_pair, action_list
			f.write(key_pair[0] + '\t' + key_pair[1] + '\t' + str(cal_grade(action_list)) + '\n')
			# f.write(key_pair[0] + '|' + key_pair[1] + '|')
			# for action in action_list:
			# 	f.write(str(action) + ' ')
			# f.write('\n')

def save_with_format(rs_dict, filename):
	'''
	save_with_format(rs_dict, filename)
		rs_dict => data to be wrote to file
			key => u_id
			value => brand_id_list
		filename => you know that

	Save dict to file with right format (u_id	brand_id, brand_id...)	
	'''
	with open(filename, 'w') as f:
		for u_id, brand_id_list in rs_dict.items():
			f.write(str(u_id) + '\t')
			brand_id_str = ''
			for brand_id in brand_id_list:
				brand_id_str = brand_id_str + str(brand_id) + ','
			f.write(brand_id_str[: -1] + '\n')

def gen_final_result(source, des):
	'''
	gen_real_result(source, des)
		source => initial data file.
		des => real result with right format.

	Generate the real result(buy only) of the last month with right format.
		u_id	brand_id,brand_id,brand_id
	'''
	rs = {}
	with open(source) as f:
		for line in f:
			temp_list = line.split()
			# if int(temp_list[2]) == 1: #or int(temp_list[2] == 2) or int(temp_list[2] == 3): # standard
			if not temp_list[0] in rs:
				rs[temp_list[0]] = []
			if not temp_list[1] in rs[temp_list[0]]:
				rs[temp_list[0]].append(temp_list[1])
	save_with_format(rs, des)

def recommend(gradefile, rec_result, threshold=0.75, filter_length=20):
	'''
	recommend(gradefile, simpleRec, threshold=0.75, filter_length=30)
		gradefile => file generated by gen_grade_file
		rec_result => recommendation result
		threshold => recommend threshold, default value: 0.75
		filter_length => filter length, default value: 30

	Generate a simple recommendation result.
		grade > threshold ? recommend
	'''
	rs = {}
	with open(gradefile) as f:
		for line in f:
			temp_list = line.split()
			if float(temp_list[2]) >= threshold:
				if not temp_list[0] in rs:
					rs[temp_list[0]] = []
				if not (temp_list[1], temp_list[2]) in rs[temp_list[0]]:
					rs[temp_list[0]].append((temp_list[1], float(temp_list[2])))

	with open(rec_result, 'w') as f:
		for u_id, brand_id_list in rs.items():
			f.write(u_id + '\t')
			brand_id_str = ''
			# sort by the grade, lower grade last
			brand_id_list = sorted(brand_id_list, key=lambda pair: pair[1], reverse=True)
			for brand_id in brand_id_list:
				brand_id_str = brand_id_str + str(brand_id[0]) + ','
			f.write(brand_id_str[: -1] + '\n')


	# with open('neverBuy.txt', 'w') as f:
	# 	for u_id, brand_id_list in rs.items():
	# 		if isNeverBuy(brand_id_list): # is never buy
	# 			f.write(u_id + '\t')
	# 			brand_id_str = ''
	# 			for brand_id in brand_id_list:
	# 				brand_id_str = brand_id_str + str(brand_id) + ','
	# 			f.write(brand_id_str[: -1] + '\n')

	# with open('simple_with_grades.txt', 'w') as f:
	# 	for u_id, brand_id_list in rs.items():
	# 		f.write(u_id + '\t')
	# 		brand_id_str = ''
	# 		for brand_id in brand_id_list:
	# 			brand_id_str = brand_id_str + str(brand_id) + ','
	# 		f.write(brand_id_str[: -1] + '\n')

def load_result(filename):
	'''
	load_result(filename)
		filename => result file with format (u_id	brand_id,brand_id...)
		return value => dict
							key => u_id
							value => set of brand_id

	Load predicted result or real result from file to dict.
	'''
	rs = {}
	with open(filename) as f:
		for line in f:
			temp_list = line.split()
			rs[temp_list[0]] = set(temp_list[1].split(','))
	return rs

def load_result_list(filename):
	'''
	load_result(filename)
		filename => result file with format (u_id	brand_id,brand_id...)
		return value => dict
							key => u_id
							value => set of brand_id

	Load predicted result or real result from file to dict.
	'''
	rs = {}
	with open(filename) as f:
		for line in f:
			temp_list = line.split()
			rs[temp_list[0]] = temp_list[1].split(',')
	return rs

def cal_f1(rec_result, real_result):
	'''
	cal_f1(rec_result, real_result)
		rec_result => recommendation result file
		real_result => real result file

	Calculate the F1.
		F1 = (2 * P * R) / (P + R)
	'''
	predict_dict = load_result(rec_result)
	real_dict = load_result(real_result)
	# calculate the precision
	num_predict_brand = 0.0
	num_hit_brand = 0.0
	for u_id, brand_id_set in predict_dict.items():
		num_predict_brand += len(brand_id_set)
		if u_id in real_dict:
			# if len(real_dict[u_id].intersection(brand_id_set)) > 0:
			# 	print real_dict[u_id], brand_id_set
			num_hit_brand += len(real_dict[u_id].intersection(brand_id_set))
		else:
			num_hit_brand += 0.0
	print 'hit brand: ', num_hit_brand, 'predict brand: ', num_predict_brand
	precision = num_hit_brand / num_predict_brand
	print 'precision: ', precision * 100, '%'
	# calculate the recall
	num_real_brand = 0.0
	num_hit_brand = 0.0
	for u_id, brand_id_set in real_dict.items():
		num_real_brand += len(brand_id_set)
		if u_id in predict_dict:
			num_hit_brand += len(predict_dict[u_id].intersection(brand_id_set))
		else:
			num_hit_brand += 0.0
	print 'hit brand: ', num_hit_brand, 'real brand: ', num_real_brand
	recall = num_hit_brand / num_real_brand
	print 'recall: ', recall * 100, '%'
	# calculate the F1
	if num_hit_brand == 0:
		print 'Floating point exception.'
		return 0
	else:
		return (2 * precision * recall) / (precision + recall)
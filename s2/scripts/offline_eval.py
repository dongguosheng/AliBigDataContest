# -*- coding: utf-8 -*-

from datetime import datetime
import math

threshold = 800000 
# 1023035

def cal_f1():
	print 'Cal F1 ...'
	num_hit = int(sql('select count(*) from (select a.user_id, a.brand_id from t_4p1_offline_real_result a join (select user_id, brand_id from (select * from t_offline_pair_proba_test_combine order by (proba0+proba1) desc limit ' + str(threshold) + ')e ) b on a.user_id=b.user_id and a.brand_id=b.brand_id) c').split()[-1])
	
	num_predict = threshold
	
	num_real = 1023035
	
	p = float(num_hit) / num_predict
	r = float(num_hit) / num_real
	
	print 'Predict number: ' + str(num_predict)
	print 'Precision: ' + str(p*100) + '%'
	print 'Recall: ' + str(r*100) + '%'
	
	print 'F1 score: ' + str(r * p * 2 / (p + r) * 100) + '%'

def main():
	cal_f1()
	
if __name__ == '__main__':
	main()

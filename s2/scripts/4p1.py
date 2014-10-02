# -*- coding: utf-8 -*-

from datetime import datetime
import json
import os

# load config.json file to config dict
# print os.getcwd()
config = json.load(open('./scripts_guosheng/4p1config.json'))
config['dim_num'] = len(config['col_name_list'])
config['col_index'] = [i for i in range(2, len(config['col_name_list']) + 2)]
print 'dim num: ' + str(config['dim_num'])

def getValue(key):
	return str(config[key])

def normalize(input, output):
	'''
	Normalize train and test.
	'''
	print 'Normalizing ' + input + ' ...'
	sql(str('drop table if exists ' + output + ';'))
	DataProc.normalize(input, output, selectedColNames=config['col_name_list'])
	
def gen_train_test():
	'''
	生成Train Set 和 Test Set.
	'''
	# Split to get train and test
	print 'Split to get train and test set ...'
	sql(str('drop table if exists ' + getValue('train_without_label') + '; drop table if exists ' + getValue('test')))
	sql(str('create table ' + getValue('train_without_label') + ' as select user_id, brand_id, ' + ','.join(config['col_name_list']) + ' from ' + getValue('train_test_norm') + ' where flag = 1;'))
	sql(str('create table ' + getValue('test') + ' as select user_id, brand_id, ' + ','.join(config['col_name_list']) + ' from ' + getValue('train_test_norm') + ' where flag = 0;'))
	
	# RF NO NEED TO SPARSE !!!
	# Test to sparse
	#print 'Test set to sparse'
	#sql(str('drop table if exists ' + config['test_sparse'] + ';'))
	#DataConvert.tableToSparseMatrix(str(config['test']), str(config['test_sparse']), selectedColIndex=config['col_index'])

def add_label():
	'''
	给Train Set 加Label.
	'''
	print 'Adding label with '+ getValue('label_table') + ' ...'
	sql(str('drop table if exists ' + getValue('train') + ';'))
	sql(str('create table ' + getValue('train') + ' as select a.*, b.label from ' + getValue('train_without_label') + ' a left outer join ' + getValue('label_table') + ' b on a.user_id=b.user_id and a.brand_id=b.brand_id;'))
	
def sample_train_predict(i):
	print i
	sample_num = int(sql('select count(*) from ' + getValue('train') + ' where label = 1;').split()[-1])
	print 'Table Name: ' + getValue('train')
	# 开始采样
	sql('drop table if exists ' + getValue('label_0') + ';')
	print 'Get label 0 ...'
	sql('create table ' + getValue('label_0') + ' as select user_id, brand_id, ' + str(','.join(config['col_name_list'])) + ', label from ' + getValue('train') + ' where label=0;')
	sql('drop table if exists ' + getValue('sample_0') + ';')
	print 'Sampling ...'
	sample_num = sample_num * 16
	print 'Sample Number: ' + str(sample_num)
	DataProc.Sample.randomSample(getValue('label_0'), sample_num, getValue('sample_0'))
	sql('drop table if exists ' + getValue('label_0') + ';')
	sql('''
	insert into table ''' + getValue('sample_0') + '''
	select user_id, brand_id, ''' + str(','.join(config['col_name_list'])) + ''', label from ''' + getValue('train') + '''
	where label='1';
	
	drop table if exists ''' + getValue('sample_0_1') + ''';
	alter table ''' + getValue('sample_0') + ''' rename to ''' + getValue('sample_0_1') + ''';
	''')
	# 采样结束

	# RF NO NEED TO SPARSE
	#sql('drop table if exists ' + getValue('train_sparse') + ';')
	#print 'Train to sparse ...'
	#DataConvert.tableToSparseMatrix(getValue('sample_0_1'), getValue('train_sparse'), selectedColIndex=config['col_index'])
	
	#rf(i)
	gbrt(i)
	
	if i == 0:
		sql('drop table if exists t_pair_proba;')
		DataProc.appendColumns([getValue('test'), 't_proba_gbrt'], 't_pair_proba', selectedColNamesList = [['user_id', 'brand_id'], ['proba' + str(i)]])
	else:
		sql('drop table if exists t_pair_proba_temp;')
		DataProc.appendColumns(['t_pair_proba', 't_proba_gbrt'], 't_pair_proba_temp')
		sql('drop table if exists t_pair_proba; alter table t_pair_proba_temp rename to t_pair_proba;')	

def rf(i):
	now = datetime.now()
	print 'RF'
	sql('drop table if exists ' + getValue('model_table') + ';')
	print 'RF training ...'
	rf_model = Classification.RandomForest.train(getValue('sample_0_1'), config['col_name_list'], [True for j in range(config['dim_num'])], 'label', getValue('model_table'), 100, algorithmTypes=[0, 100])
	sql('drop table if exists t_proba_rf;')
	print 'RF predicting ...'
	Classification.RandomForest.predict(getValue('test'), rf_model, 't_proba_rf', appendColNames=['user_id', 'brand_id'], labelValueToPredict='1')
	sql('alter table t_proba_rf change column probability rename to proba' + str(i) + ';')
	print datetime.now() - now			

def gbrt(i):
	sql('drop table if exists ' + getValue('train_sparse') + ';')
	print 'Train to sparse ...'
	DataConvert.tableToSparseMatrix(getValue('sample_0_1'), getValue('train_sparse'), selectedColIndex=config['col_index'])

	# 开始训练
	sql('drop table if exists ' + getValue('model_table') + ';')
	print 'Training gbrt model ...'
	model_gbrt = Regression.GradBoostRegTree.trainSparse(getValue('train_sparse'), getValue('sample_0_1'), 'label', 't_model_gbrt', treeDepth=8, treesNum=500, learningRate=0.1)
	# model_gbrt = Regression.GradBoostRegTree.loadModel(getValue('model_table'))
	
	# Test to sparse
	print 'Test set to sparse'
	sql(str('drop table if exists ' + getValue('test_sparse') + ';'))
	DataConvert.tableToSparseMatrix(str(getValue('test')), str(getValue('test_sparse')), selectedColIndex=config['col_index'])
	
	# 开始预测
	sql('drop table if exists t_proba_gbrt;')
	print 'Predicting ...'
	Regression.GradBoostRegTree.predictSparse(getValue('test_sparse'), model_gbrt, 't_proba_gbrt')
	sql('alter table t_proba_gbrt change column y_var rename to proba' + str(i) + ';')

def gen_final_rs():
	print 'Gen Final Result ...'
	sql('''
		drop table if exists t_tmall_add_user_brand_predict_dh;
		create table t_tmall_add_user_brand_predict_dh as
		select user_id, wm_concat(',', brand_id) as brand
		from(
			select *
			from t_pair_proba
			order by proba0 desc
			limit 2716354 
			)a
		group by user_id;
	''')
	# 2835057
def main():
	now = datetime.now()
	# Normalize
	#normalize(str(getValue('train_test_without_norm')), str(getValue('train_test_norm')))
	print datetime.now() - now
	
	#gen_train_test()
	print datetime.now() - now
	#add_label()
	
	print datetime.now() - now
	for i in range(0, 5):
		sample_train_predict(i)
		print datetime.now() - now
	
	#gen_final_rs()

if __name__ == '__main__':
	main()

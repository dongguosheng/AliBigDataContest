# -*- coding: utf-8 -*-

from datetime import datetime
import json

def init_config(path):
	config = json.load(open(path))
	config['dim_num'] = len(config['col_name_list'])
	config['col_index'] = [i for i in range(2, len(config['col_name_list']) + 2)]
	print 'Dim num: ' + str(config['dim_num'])
	return config

def getValue(key, config):
	return str(config[key])

def split_user(config):
	sql('drop table if exists ' + getValue('user_table', config) + ';')
	sql('create table ' + getValue('user_table', config) + ' as select distinct user_id from ' + getValue('train_test_norm', config) + ';')
	sample_num = int(sql('select count(*) from ' + getValue('user_table', config) + ';').split()[-1])
	sql('drop table if exists ' + getValue('user_train', config) + ';')
	DataProc.Sample.randomSample(getValue('user_table', config), sample_num / 2, getValue('user_train', config))
	
	sql('drop table if exists ' + getValue('user_test', config) + ';')
	sql('create table ' + getValue('user_test', config) + ' as select user_id from (select a.user_id as flag, b.user_id from ' + getValue('user_train', config) + ' a right outer join ' + getValue('user_table', config) + ' b on a.user_id=b.user_id) c where c.flag is null;')

def gen_train_test(config):
	# Gen offline train test set
	sql(str('drop table if exists ' + getValue('train_test_without_norm', config) + ';'))
	s_train = 'create table ' + getValue('train_test_without_norm', config) + ' as select user_id, brand_id, ' + ','.join(config['col_name_list']) + ' from ' + getValue('input_table', config) + ' where flag = 1;'
	
	print s_train
	sql(str(s_train))
	
	normalize(getValue('train_test_without_norm', config), getValue('train_test_norm', config), config)
	
def normalize(input, output, config):
	'''
	Normalize train and test.
	'''
	print 'Normalizing ' + input + ' ...'
	sql(str('drop table if exists ' + output + ';'))
	# Normalize
	DataProc.normalize(input, output, selectedColNames=config['col_name_list'])
	# Not Normalize
	#print 'Not Normalize'
	#sql(str('create table ' + output + ' as select * from ' + input + ';'))
	
def split_train_test(config):
	# Gen offline train
	print 'Gen offline train'
	sql('drop table if exists ' + getValue('train_without_label', config) + ';')
	str_sql = 'create table ' + getValue('train_without_label', config) + ' as select b.user_id, b.brand_id, ' + ','.join(['b.'+s for s in config['col_name_list']])  + ' from ' + getValue('user_train', config) + ' a left outer join ' + getValue('train_test_norm', config) + ' b on a.user_id=b.user_id;'
	print str_sql
	sql(str(str_sql))
	
	# Gen offline test
	print 'Gen offline test'
	sql('drop table if exists ' + getValue('test', config) + ';')
	sql(str('create table ' + getValue('test', config) + ' as select b.user_id, b.brand_id, ' + ','.join(['b.'+s for s in config['col_name_list']])  + ' from ' + getValue('user_test', config) + ' a left outer join ' + getValue('train_test_norm', config) + ' b on a.user_id=b.user_id;'))
	
	# 使用RF不需要转稀疏矩阵
	
def add_label(config):
	# Add label
	print 'Adding label ...' + getValue('label_table', config)
	sql('drop table if exists ' + getValue('train', config) + ';')
	sql('create table ' + getValue('train', config) + ' as select a.*, b.label from ' + getValue('train_without_label', config) + ' a left outer join ' + getValue('label_table', config) + ' b on a.user_id=b.user_id and a.brand_id=b.brand_id;')
	
def sample_train_predict(i, config):
	print i
	sample_num = int(sql('select count(*) from ' + getValue('train', config) + ' where label = 1;').split()[-1])
	print 'Table Name: ' + getValue('train', config)
	# 开始采样
	sql('drop table if exists ' + getValue('label_0', config) + ';')
	print 'Get label 0 ...'
	sql('create table ' + getValue('label_0', config) + ' as select user_id, brand_id, ' + str(','.join(config['col_name_list'])) + ', label from ' + getValue('train', config) + ' where label=0;')
	sql('drop table if exists ' + getValue('sample_0', config) + ';')
	print 'Sampling ...'
	sample_num = int(sample_num * config['gbrt']['sample_ratio'])
	print 'Sample ratio: ' + str(config['gbrt']['sample_ratio'])
	print 'Sample Number: ' + str(sample_num)
	DataProc.Sample.randomSample(getValue('label_0', config), sample_num, getValue('sample_0', config))
	sql('drop table if exists ' + getValue('label_0', config) + ';')
	sql('''
	insert into table ''' + getValue('sample_0', config) + '''
	select user_id, brand_id, ''' + str(','.join(config['col_name_list'])) + ''', label from ''' + getValue('train', config) + '''
	where label='1';
	
	drop table if exists ''' + getValue('sample_0_1', config) + ''';
	alter table ''' + getValue('sample_0', config) + ''' rename to ''' + getValue('sample_0_1', config) + ''';''')
	# 采样结束

	rf(i, config)
	#gbrt(i, config)

	if i == 0:
		sql('drop table if exists t_offline_pair_proba;')
		DataProc.appendColumns([getValue('test', config), getValue('rf_proba_table', config)], 't_offline_pair_proba', selectedColNamesList = [['user_id', 'brand_id'], ['proba' + str(i)]])
	else:
		sql('drop table if exists t_offline_pair_proba_temp;')
		DataProc.appendColumns(['t_offline_pair_proba', getValue('gbrt_proba_table', config)], 't_offline_pair_proba_temp')
		sql('drop table if exists t_offline_pair_proba; alter table t_offline_pair_proba_temp rename to t_offline_pair_proba;')	

def rf(i, config):
	now = datetime.now()
	print 'RF'
	sql('drop table if exists ' + getValue('rf_model_table', config) + ';')
	print 'RF training ...'
	rf_model = Classification.RandomForest.train(getValue('sample_0_1', config), config['col_name_list'], [True for j in range(config['dim_num'])], 'label', getValue('rf_model_table', config), 300, maxTreeDeep=20, algorithmTypes=[300, 300])
	sql('drop table if exists ' + getValue('rf_proba_table', config) + ';')
	print 'RF predicting ...'
	Classification.RandomForest.predict(getValue('test', config), rf_model, getValue('rf_proba_table', config), appendColNames=['user_id', 'brand_id'], labelValueToPredict='1')
	sql('alter table ' + getValue('rf_proba_table', config) + ' change column probability rename to proba' + str(i) + ';')
	print datetime.now() - now	
	
def lr(i, config):
	print 'LR ...'
	sql('drop table if exists ' + getValue('lr_model_table', config) + ';')
	print 'LR training ...'
	lr_model = Classification.LogistReg.train(getValue('sample_0_1', config), config['col_name_list'], 'label', getValue('lr_model_table', config), goodValue='1', maxIter=150, epsilon=1.0e-05, regularizedType='l1', regularizedLevel=2.0)
	sql('drop table if exists ' + getValue('lr_proba_table', config) + ';')
	print 'LR predicting ...'	
	Classification.LogistReg.predict(getValue('test', config), lr_model, getValue('lr_proba_table', config), labelValueToPredict='1')
	sql('alter table ' + getValue('lr_proba_table', config) + ' change column probability rename to proba' + str(i) + ';')
		
def gbrt(i, config):
	print 'GBRT ...'
	sql('drop table if exists ' + getValue('train_sparse', config) + ';')
	print 'Train to sparse ...'
	DataConvert.tableToSparseMatrix(getValue('sample_0_1', config), getValue('train_sparse', config), selectedColIndex=config['col_index'])
	
	# 开始训练
	sql('drop table if exists ' + getValue('gbrt_model_table', config) + ';')
	print 'Training gbrt model ...'
	print 'Tree depth: ' + str(config['gbrt']['tree_depth'])
	print 'Tree num: ' + str(config['gbrt']['tree_num'])
	model_gbrt = Regression.GradBoostRegTree.trainSparse(getValue('train_sparse', config), getValue('sample_0_1', config), 'label', getValue('gbrt_model_table', config), treeDepth=config['gbrt']['tree_depth'], treesNum=config['gbrt']['tree_num'], learningRate=0.1)
	# model_gbrt = Regression.GradBoostRegTree.loadModel(getValue('model_table'))
	
	sql('drop table if exists ' + getValue('test_sparse', config) + ';')
	print 'Test set to sparse'
	DataConvert.tableToSparseMatrix(getValue('test', config), getValue('test_sparse', config), selectedColIndex=config['col_index'])
	
	# 开始预测
	sql('drop table if exists ' + getValue('gbrt_proba_table', config) + ';')
	print 'Predicting ...'
	Regression.GradBoostRegTree.predictSparse(getValue('test_sparse', config), model_gbrt, getValue('gbrt_proba_table', config))
	sql('alter table ' + getValue('gbrt_proba_table', config) + ' change column y_var rename to proba' + str(i) + ';')
		
def gen_real_result(config):
	print 'gen real result ...'
	sql('drop table if exists t_4p1_offline_real_result;')
	sql('create table t_4p1_offline_real_result as select user_id, brand_id from (select a.user_id as flag1, a.brand_id as flag2, b.user_id, b.brand_id from ' + getValue('train', config) + ' a right outer join (select user_id, brand_id from t_4p1_label_buy where label = 1) b on a.user_id=b.user_id and a.brand_id=b.brand_id) c where c.flag1 is null and c.flag2 is null;')
	
def cal_f1(config):
	print 'Cal F1 ...'
	num_hit = int(sql('select count(*) from (select a.user_id, a.brand_id from ' + getValue('real_result', config) + ' a join (select user_id, brand_id from (select * from ' + getValue('predict_result', config) + ' order by ' + getValue('proba_col_name', config) + ' desc limit ' + getValue('threshold', config) + ')e ) b on a.user_id=b.user_id and a.brand_id=b.brand_id) c').split()[-1])
	
	num_predict = int(getValue('threshold', config))
	
	num_real = 1023035
	
	p = float(num_hit) / num_predict
	r = float(num_hit) / num_real
	
	with open(str(datetime.now()).replace(':', '-') + '.txt', 'w') as f:
		f.write('Config: \nThreshold: ' + getValue('threshold', config))
		f.write('\nDim num: ' + getValue('dim_num', config))
		f.write('\nTree depth: ' + str(config['gbrt']['tree_depth']))
		f.write('\nTree num: ' + str(config['gbrt']['tree_num']))
		f.write('\n\nPredict num: ' + str(num_predict))
		f.write('\nPrecision: ' + str(p*100) + '%')
		f.write('\nRecal: ' + str(r*100) + '%')
		f.write('\nF1 score: ' + str(r * p * 2 / (p + r) * 100) + '%')
	
	print 'Predict number: ' + str(num_predict)
	print 'Precision: ' + str(p*100) + '%'
	print 'Recall: ' + str(r*100) + '%'
	
	print 'F1 score: ' + str(r * p * 2 / (p + r) * 100) + '%'
		
def main():
	# gen_train_test() 只需要在新加了特征的时候调用一次产生一个大表
	#all_feature_path = './scripts_guosheng/4p1_expand_22offline_config.json'
	#gen_train_test(init_config(all_feature_path))

	# ------------------------------------------------------------------------------------------
	
	# 拆分train and test set
	now = datetime.now()
	config = init_config('./scripts_guosheng/4p1_expand_22offline_config.json')	
	
	# Only need at first time
	print 'Split user to train and test ...'
	#split_user(config)
	print datetime.now() - now
	
	print 'Split train and test ...'
	#split_train_test(config)
	print datetime.now() - now
	
	print 'Adding label ...'
	#add_label(config)
	print datetime.now() - now
	
	print 'Sample train and predicting ...'
	for i in range(1):
		sample_train_predict(i, config)
		print datetime.now() - now
	
	# Only need at first time
	#gen_real_result(config)
	

	cal_f1(config)
	print datetime.now() - now
	
	#rf(0, config)
	#gbrt(0)

	#sql('drop table if exists t_offline_pair_proba;')
	#DataProc.appendColumns([getValue('test', config), getValue('rf_proba_table', config)], 't_offline_pair_proba', selectedColNamesList = [['user_id', 'brand_id'], ['proba0']])
	#cal_f1(config)
	
if __name__ == '__main__':
	main()

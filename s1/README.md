# 比赛第一季
## Workflow:
* step0:	原始数据文件的时间项已再excel中做过格式转换，并按月份split为前三个月、后三个月和最后一个月。前三个月为训练集，最后一个月的部分满足操作条件的（uid，brandid）的作为Label。后三个月数据为测试集。
* step1:	生成标准答案
    `./gen_real_result.py ./dataset/last_month_with_date.data ./dataset/real_result_larger_4.txt`
* step2:	使用step1产生的标准答案，生成约定格式训练集；生成约定格式测试集
    `./gen_features.py ./dataset/three_month_with_date.data grades_3.txt front ./dataset/real_result_larger_4.txt`
	`./gen_features.py ./dataset/last_three_month.txt grades_3_last.txt last ./dataset/real_result_larger_4.txt`
* step3:	多次有放回随机正负样本采样，需指定比例
	`./sample.py grades_3.txt 1`
* step4:	在多个采样训练集上进行训练，并在测试集上进行测试，生成多个分类结果；对多个分类结果进行融合，生成最终可提交的版本
	`./multi_sample_gbc.sh`

# 文件构成：
* tmall.py	Some Util functions
* gen_real_result.py	生成标准答案
* gen_features.py	生成训练集或测试集
* sample.py	正负样本采样
* gbc.py		使用sklearn.ensemble.GradientBoostingClassifier训练模型并预测
* traverse_dir.py	多次采样并对结果进行融合
* multi_sample_gbc.sh	多次训练并对预测结果进行融合生成可提交格式的bash

队伍信息：
周兴，董国盛，吴波
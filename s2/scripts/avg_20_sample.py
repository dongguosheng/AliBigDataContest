sql('drop table if exists t_pair_proba_norm_lr;')
DataProc.normalize('t_pair_proba', 't_pair_proba_norm_gbrt', selectedColNames=['proba0', 'proba1', 'proba2', 'proba3', 'proba4', 'proba5', 'proba6', 'proba7', 'proba8', 'proba9', 'proba10', 'proba11', 'proba12', 'proba13', 'proba14', 'proba15', 'proba16', 'proba17', 'proba18', 'proba19'])
sql('''
	drop table if exists t_proba_gbrt;
	create table t_proba_gbrt as
	select user_id, brand_id, (proba0+proba1+proba2+proba3+proba4+proba5+proba6+proba7+proba8+proba9+proba10+proba11+proba12+proba13+proba14+proba15+proba16+proba17+proba18+proba19)/20 as proba
	from t_pair_proba_norm_gbrt;
''')


# 改变t_proba_gbrt的列名
def gen_final_rs():
	sql('''
		drop table if exists t_tmall_add_user_brand_predict_dh;
		create table t_tmall_add_user_brand_predict_dh as
		select user_id, wm_concat(',', brand_id) as brand
		from(
			select *
			from t_pair_proba
			order by (proba0+proba1+proba2+proba3+proba4+proba5+proba6+proba7+proba8+proba9+proba10) desc
			limit 2600000
			)a
		group by user_id;
	''')

gen_final_rs()

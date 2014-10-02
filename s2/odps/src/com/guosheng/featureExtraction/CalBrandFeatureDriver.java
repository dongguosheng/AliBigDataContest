package com.guosheng.featureExtraction;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class CalBrandFeatureDriver {

	public static void main(String[] args) throws OdpsException {
		// 使用SQLTask创建输出表
//		Account account = new AliyunAccount("OBNp6MB6cfSwNeCq",
//				"BiSNYpp40o47agSE74raqPgGNgA0gB");
//		Odps odps = new Odps(account);
//		if (!odps.tables().exists("t_4p1_feature_brand")) {
//			String sql = "create table t_4p1_feature_brand (flag bigint, brand_id string, user_id string, brand_click double, brand_buy double, brand_favorite double, brand_cart double, brand_click_user_num double, brand_buy_user_num double, brand_favorite_user_num double, brand_cart_user_num double, brand_effective_click double, brand_effective_favorite double, brand_effective_cart double, brand_time_between_buy double, brand_avg_click_with_buy double, brand_avg_favorite_with_buy double, brand_avg_cart_with_buy double, brand_avg_time_between_buy double)";
//			Instance instance = SQLTask.run(odps, sql);
//			// TODO
//		}
//		brand feature不受pairFeatureLength的影响

//		create table t_4p1_feature_user_brand as select a.*, b.brand_click, b.brand_buy, b.brand_favorite, b.brand_cart, b.brand_click_user_num, b.brand_buy_user_num, b.brand_favorite_user_num, b.brand_cart_user_num, b.brand_effective_click, b.brand_effective_favorite, b.brand_effective_cart,b.brand_time_between_buy, b.brand_avg_click_with_buy, b.brand_avg_favorite_with_buy, b.brand_avg_cart_with_buy,b.brand_avg_time_between_buy from t_4p1_feature_pair_user a left outer join t_4p1_feature_brand b on a.user_id=b.user_id and a.brand_id=b.brand_id and a.flag= b.flag
//		create table t_4p1_feature_user_brand_14 as select a.*, b.brand_click, b.brand_buy, b.brand_favorite, b.brand_cart, b.brand_click_user_num, b.brand_buy_user_num, b.brand_favorite_user_num, b.brand_cart_user_num, b.brand_effective_click, b.brand_effective_favorite, b.brand_effective_cart,b.brand_time_between_buy, b.brand_avg_click_with_buy, b.brand_avg_favorite_with_buy, b.brand_avg_cart_with_buy,b.brand_avg_time_between_buy from t_4p1_feature_pair_user_14 a left outer join t_4p1_feature_brand b on a.user_id=b.user_id and a.brand_id=b.brand_id and a.flag= b.flag
//		create table t_4p1_feature_user_brand_12 as select a.*, b.brand_click, b.brand_buy, b.brand_favorite, b.brand_cart, b.brand_click_user_num, b.brand_buy_user_num, b.brand_favorite_user_num, b.brand_cart_user_num, b.brand_effective_click, b.brand_effective_favorite, b.brand_effective_cart,b.brand_time_between_buy, b.brand_avg_click_with_buy, b.brand_avg_favorite_with_buy, b.brand_avg_cart_with_buy,b.brand_avg_time_between_buy from t_4p1_feature_pair_user_12 a left outer join t_4p1_feature_brand b on a.user_id=b.user_id and a.brand_id=b.brand_id and a.flag= b.flag
		
		JobConf job = new JobConf();	
		job.setInt("pairFeatureLength", 22);
		String schemaStr22 = "user_id:string,f1_0:bigint,f1_1:bigint,f1_2:bigint,f1_3:bigint,f2_0:bigint,f2_1:bigint,f2_2:bigint,f2_3:bigint,f3_0:bigint,f3_1:bigint,f3_2:bigint,f3_3:bigint,f4_0:bigint,f4_1:bigint,f4_2:bigint,f4_3:bigint,f5_0:bigint,f5_1:bigint,f5_2:bigint,f5_3:bigint,f6_0:bigint,f6_1:bigint,f6_2:bigint,f6_3:bigint,f7_0:bigint,f7_1:bigint,f7_2:bigint,f7_3:bigint,f8_0:bigint,f8_1:bigint,f8_2:bigint,f8_3:bigint,f9_0:bigint,f9_1:bigint,f9_2:bigint,f9_3:bigint,f10_0:bigint,f10_1:bigint,f10_2:bigint,f10_3:bigint,f11_0:bigint,f11_1:bigint,f11_2:bigint,f11_3:bigint,f12_0:bigint,f12_1:bigint,f12_2:bigint,f12_3:bigint,f13_0:bigint,f13_1:bigint,f13_2:bigint,f13_3:bigint,f14_0:bigint,f14_1:bigint,f14_2:bigint,f14_3:bigint,f15_0:bigint,f15_1:bigint,f15_2:bigint,f15_3:bigint,f16_0:bigint,f16_1:bigint,f16_2:bigint,f16_3:bigint,f17_0:bigint,f17_1:bigint,f17_2:bigint,f17_3:bigint,f18_0:bigint,f18_1:bigint,f18_2:bigint,f18_3:bigint,f19_0:bigint,f19_1:bigint,f19_2:bigint,f19_3:bigint,f20_0:bigint,f20_1:bigint,f20_2:bigint,f20_3:bigint,f21_0:bigint,f21_1:bigint,f21_2:bigint,f21_3:bigint,f22_0:bigint,f22_1:bigint,f22_2:bigint,f22_3:bigint,click_before_buy:double,favorite_before_buy:double,cart_before_buy:double,click_after_buy:double,favorite_after_buy:double,cart_after_buy:double,time_between_buy:double,time_after_buy:double";
		String schemaStr18 = "user_id:string,f1_0:bigint,f1_1:bigint,f1_2:bigint,f1_3:bigint,f2_0:bigint,f2_1:bigint,f2_2:bigint,f2_3:bigint,f3_0:bigint,f3_1:bigint,f3_2:bigint,f3_3:bigint,f4_0:bigint,f4_1:bigint,f4_2:bigint,f4_3:bigint,f5_0:bigint,f5_1:bigint,f5_2:bigint,f5_3:bigint,f6_0:bigint,f6_1:bigint,f6_2:bigint,f6_3:bigint,f7_0:bigint,f7_1:bigint,f7_2:bigint,f7_3:bigint,f8_0:bigint,f8_1:bigint,f8_2:bigint,f8_3:bigint,f9_0:bigint,f9_1:bigint,f9_2:bigint,f9_3:bigint,f10_0:bigint,f10_1:bigint,f10_2:bigint,f10_3:bigint,f11_0:bigint,f11_1:bigint,f11_2:bigint,f11_3:bigint,f12_0:bigint,f12_1:bigint,f12_2:bigint,f12_3:bigint,f13_0:bigint,f13_1:bigint,f13_2:bigint,f13_3:bigint,f14_0:bigint,f14_1:bigint,f14_2:bigint,f14_3:bigint,f15_0:bigint,f15_1:bigint,f15_2:bigint,f15_3:bigint,f16_0:bigint,f16_1:bigint,f16_2:bigint,f16_3:bigint,f17_0:bigint,f17_1:bigint,f17_2:bigint,f17_3:bigint,f18_0:bigint,f18_1:bigint,f18_2:bigint,f18_3:bigint,click_before_buy:double,favorite_before_buy:double,cart_before_buy:double,click_after_buy:double,favorite_after_buy:double,cart_after_buy:double,time_between_buy:double,time_after_buy:double";
		String schemaStr16 = "user_id:string,f1_0:bigint,f1_1:bigint,f1_2:bigint,f1_3:bigint,f2_0:bigint,f2_1:bigint,f2_2:bigint,f2_3:bigint,f3_0:bigint,f3_1:bigint,f3_2:bigint,f3_3:bigint,f4_0:bigint,f4_1:bigint,f4_2:bigint,f4_3:bigint,f5_0:bigint,f5_1:bigint,f5_2:bigint,f5_3:bigint,f6_0:bigint,f6_1:bigint,f6_2:bigint,f6_3:bigint,f7_0:bigint,f7_1:bigint,f7_2:bigint,f7_3:bigint,f8_0:bigint,f8_1:bigint,f8_2:bigint,f8_3:bigint,f9_0:bigint,f9_1:bigint,f9_2:bigint,f9_3:bigint,f10_0:bigint,f10_1:bigint,f10_2:bigint,f10_3:bigint,f11_0:bigint,f11_1:bigint,f11_2:bigint,f11_3:bigint,f12_0:bigint,f12_1:bigint,f12_2:bigint,f12_3:bigint,f13_0:bigint,f13_1:bigint,f13_2:bigint,f13_3:bigint,f14_0:bigint,f14_1:bigint,f14_2:bigint,f14_3:bigint,f15_0:bigint,f15_1:bigint,f15_2:bigint,f15_3:bigint,f16_0:bigint,f16_1:bigint,f16_2:bigint,f16_3:bigint,click_before_buy:double,favorite_before_buy:double,cart_before_buy:double,click_after_buy:double,favorite_after_buy:double,cart_after_buy:double,time_between_buy:double,time_after_buy:double";
		
		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("flag:bigint,brand_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString(schemaStr22));

		// TODO: specify input and output tables
		String inputTable = "t_4p1_feature_22";
		String outputTable = "t_4p1_feature_brand_22_expand_brand_all";
		InputUtils
				.addTable(TableInfo.builder().tableName(inputTable).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(outputTable).build(),
				job);

		job.setMapperClass(com.guosheng.featureExtraction.CalBrandFeatureMap.class);
		job.setReducerClass(com.guosheng.featureExtraction.CalBrandFeatureReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}

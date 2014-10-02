package com.guosheng.featureExtraction;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class CalPairFeatureDriver {

	public static void main(String[] args) throws OdpsException {
		// 使用SQLTask创建输出表
//		Account account = new AliyunAccount("OBNp6MB6cfSwNeCq",
//				"BiSNYpp40o47agSE74raqPgGNgA0gB");
//		Odps odps = new Odps(account);
//		if (!odps.tables().exists("t_4p1_feature")) {
//			String sql = "create table t_4p1_feature (flag bigint, user_id string, brand_id string, f1_0 bigint,f1_1 bigint,f1_2 bigint,f1_3 bigint,f2_0 bigint,f2_1 bigint,f2_2 bigint,f2_3 bigint,f3_0 bigint,f3_1 bigint,f3_2 bigint,f3_3 bigint,f4_0 bigint,f4_1 bigint,f4_2 bigint,f4_3 bigint,f5_0 bigint,f5_1 bigint,f5_2 bigint,f5_3 bigint,f6_0 bigint,f6_1 bigint,f6_2 bigint,f6_3 bigint,f7_0 bigint,f7_1 bigint,f7_2 bigint,f7_3 bigint,f8_0 bigint,f8_1 bigint,f8_2 bigint,f8_3 bigint,f9_0 bigint,f9_1 bigint,f9_2 bigint,f9_3 bigint,f10_0 bigint,f10_1 bigint,f10_2 bigint,f10_3 bigint,f11_0 bigint,f11_1 bigint,f11_2 bigint,f11_3 bigint,f12_0 bigint,f12_1 bigint,f12_2 bigint,f12_3 bigint,f13_0 bigint,f13_1 bigint,f13_2 bigint,f13_3 bigint,f14_0 bigint,f14_1 bigint,f14_2 bigint,f14_3 bigint,f15_0 bigint,f15_1 bigint,f15_2 bigint,f15_3 bigint,f16_0 bigint,f16_1 bigint,f16_2 bigint,f16_3 bigint, click_before_buy double, favorite_before_buy double, cart_before_buy double, click_after_buy double, favorite_after_buy double, cart_after_buy double, time_between_buy double, time_after_buy double)";
//			Instance instance = SQLTask.run(odps, sql);
//			//TODO
//		}
//		create table t_4p1_feature_14 (flag bigint, user_id string, brand_id string, f1_0 bigint,f1_1 bigint,f1_2 bigint,f1_3 bigint,f2_0 bigint,f2_1 bigint,f2_2 bigint,f2_3 bigint,f3_0 bigint,f3_1 bigint,f3_2 bigint,f3_3 bigint,f4_0 bigint,f4_1 bigint,f4_2 bigint,f4_3 bigint,f5_0 bigint,f5_1 bigint,f5_2 bigint,f5_3 bigint,f6_0 bigint,f6_1 bigint,f6_2 bigint,f6_3 bigint,f7_0 bigint,f7_1 bigint,f7_2 bigint,f7_3 bigint,f8_0 bigint,f8_1 bigint,f8_2 bigint,f8_3 bigint,f9_0 bigint,f9_1 bigint,f9_2 bigint,f9_3 bigint,f10_0 bigint,f10_1 bigint,f10_2 bigint,f10_3 bigint,f11_0 bigint,f11_1 bigint,f11_2 bigint,f11_3 bigint,f12_0 bigint,f12_1 bigint,f12_2 bigint,f12_3 bigint,f13_0 bigint,f13_1 bigint,f13_2 bigint,f13_3 bigint,f14_0 bigint,f14_1 bigint,f14_2 bigint,f14_3 bigint, click_before_buy double, favorite_before_buy double, cart_before_buy double, click_after_buy double, favorite_after_buy double, cart_after_buy double, time_between_buy double, time_after_buy double)
//		create table t_4p1_feature_12 (flag bigint, user_id string, brand_id string, f1_0 bigint,f1_1 bigint,f1_2 bigint,f1_3 bigint,f2_0 bigint,f2_1 bigint,f2_2 bigint,f2_3 bigint,f3_0 bigint,f3_1 bigint,f3_2 bigint,f3_3 bigint,f4_0 bigint,f4_1 bigint,f4_2 bigint,f4_3 bigint,f5_0 bigint,f5_1 bigint,f5_2 bigint,f5_3 bigint,f6_0 bigint,f6_1 bigint,f6_2 bigint,f6_3 bigint,f7_0 bigint,f7_1 bigint,f7_2 bigint,f7_3 bigint,f8_0 bigint,f8_1 bigint,f8_2 bigint,f8_3 bigint,f9_0 bigint,f9_1 bigint,f9_2 bigint,f9_3 bigint,f10_0 bigint,f10_1 bigint,f10_2 bigint,f10_3 bigint,f11_0 bigint,f11_1 bigint,f11_2 bigint,f11_3 bigint,f12_0 bigint,f12_1 bigint,f12_2 bigint,f12_3 bigint, click_before_buy double, favorite_before_buy double, cart_before_buy double, click_after_buy double, favorite_after_buy double, cart_after_buy double, time_between_buy double, time_after_buy double)
		

		//TODO
		JobConf job = new JobConf();
		job.setInt("pairFeatureLength", 23);

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string,brand_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("type:string,visit_datetime:string"));
		
		// TODO: specify input and output tables
		String inputTable = "t_alibaba_bigdata_user_brand_total_1";
		String outputTable = "t_4p1_feature_23";
		InputUtils
				.addTable(TableInfo.builder().tableName(inputTable).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(outputTable).build(),
				job);

		job.setMapperClass(com.guosheng.featureExtraction.CalPairFeatureMap.class);
		job.setReducerClass(com.guosheng.featureExtraction.CalPairFeatureReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}

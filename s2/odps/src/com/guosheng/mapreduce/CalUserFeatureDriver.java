package com.guosheng.mapreduce;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class CalUserFeatureDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils
				.fromString("flag:bigint,user_id:string"));
		job.setMapOutputValueSchema(SchemaUtils
				.fromString("brand_id:string,f1_0:bigint,f1_1:bigint,f1_2:bigint,f1_3:bigint,f2_0:bigint,f2_1:bigint,f2_2:bigint,f2_3:bigint,f3_0:bigint,f3_1:bigint,f3_2:bigint,f3_3:bigint,f4_0:bigint,f4_1:bigint,f4_2:bigint,f4_3:bigint,f5_0:bigint,f5_1:bigint,f5_2:bigint,f5_3:bigint,f6_0:bigint,f6_1:bigint,f6_2:bigint,f6_3:bigint,f7_0:bigint,f7_1:bigint,f7_2:bigint,f7_3:bigint,f8_0:bigint,f8_1:bigint,f8_2:bigint,f8_3:bigint,f9_0:bigint,f9_1:bigint,f9_2:bigint,f9_3:bigint,f10_0:bigint,f10_1:bigint,f10_2:bigint,f10_3:bigint,f11_0:bigint,f11_1:bigint,f11_2:bigint,f11_3:bigint,f12_0:bigint,f12_1:bigint,f12_2:bigint,f12_3:bigint,effective_click:bigint,effective_favorite:bigint,effective_cart:bigint,ineffective_click:bigint,ineffective_favorite:bigint,ineffective_cart:bigint,time_between_buy:bigint,time_after_buy:bigint"));

		// TODO: specify input and output tables
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(com.guosheng.mapreduce.CalUserFeatureMap.class);
		job.setReducerClass(com.guosheng.mapreduce.CalUserFeatureReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}

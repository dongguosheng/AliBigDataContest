package com.guosheng.mapreduce;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class MyDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		//设定map的key的输出类型
		job.setMapOutputKeySchema(SchemaUtils.fromString("user_id:string,brand_id:string"));
		//设定map的value的输出类型
		job.setMapOutputValueSchema(SchemaUtils.fromString("type:string,visit_datetime:string"));

		// TODO: specify input and output tables
		//输入的表为args[0]
		InputUtils
				.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		//输出的表为args[1]
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
				job);

		job.setMapperClass(com.guosheng.mapreduce.MyMaper.class);
		job.setReducerClass(com.guosheng.mapreduce.MyReduce.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}

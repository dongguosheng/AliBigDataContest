package com.guosheng.featureExtraction;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;

public class CalPairFeatureMap extends MapperBase {
	Record key;
	Record value;

	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		//从该条record中提取key，并设置该key的value
		//key输出
//		key.set(0, record.getString("user_id"));
//		key.set(1, record.getString("brand_id"));
//
//		//设置value
//		value.set(0, record.getString("type"));
//		value.set(1, record.getString("visit_datetime"));
		
		key.set("user_id", record.getString(0));
		key.set("brand_id", record.getString(1));
		value.set("type", record.getString(2));
		value.set("visit_datetime", record.getString(3));
		
		context.write(key, value);
	}


}

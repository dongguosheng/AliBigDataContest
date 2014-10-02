package com.guosheng.mapreduce;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;

public class CalUserFeatureMap extends MapperBase {
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
		key.set("flag", record.getBigint(0));
		key.set("user_id", record.getString(1));
		
		value.set("brand_id", record.getString(2));
		for(int i = 0; i < 56; ++i){
			value.set(1 + i, record.getBigint(3 + i));
		}
		context.write(key, value);
	}

}

package com.guosheng.featureExtraction;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;

public class CalBrandFeatureMap extends MapperBase {
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
		key.set("brand_id", record.getString(2));
		
		int pairFeatureLength = context.getJobConf().getInt("pairFeatureLength", 16);
		value.set("user_id", record.getString(1));
		for(int i = 0; i < pairFeatureLength * 4; ++i){
			value.set(1 + i, record.getBigint(3 + i));
		}
		for(int i = 0; i < 8; ++i){
			value.set(pairFeatureLength * 4 + 1 + i, record.getDouble(pairFeatureLength * 4 + 3 + i));
		}
		context.write(key, value);
	}
}

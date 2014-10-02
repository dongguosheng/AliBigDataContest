package com.guosheng.featureExtraction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

public class CalBrandFeatureReduce extends ReducerBase {
	Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		// HashMap来存储品牌及操作
		Map<String, Long[]> brandFeatureMap = new HashMap<String, Long[]>();

		Map<String, ArrayList<Long>> featuresAddMap = new HashMap<String, ArrayList<Long>>();
		int pairFeatureLength = context.getJobConf().getInt(
				"pairFeatureLength", 16);
		int length = (pairFeatureLength * 4) * 2;
		Long[] brandFeature = initBrandFeature(length); // user的部分特征，click,buy,favorite,cart的总数和click,buy,favorite,cart的brand数
		Double[] brandAdvFeature = new Double[] { 0.0, 0.0, 0.0, 0.0};
		

		while (values.hasNext()) {
			Record val = values.next();
			String user = val.getString("user_id");
			ArrayList<Long> features = new ArrayList<Long>();
			for (int i = 0; i < 4 * pairFeatureLength; ++i) {
				features.add(val.getBigint(i + 1));
			}
		
			for (int i = 0; i < 3; ++i) {
				brandAdvFeature[i] += val.getDouble(4 * pairFeatureLength + 1 + i);
			}
			brandAdvFeature[3] += val.getDouble("time_between_buy");

			ArrayList<Long> featuresAdd = getPairFeaturesAdd(features);

			for (int i = 0; i < featuresAdd.size(); ++i) {
				Long count = featuresAdd.get(i);
				if (i % 4 == 0 && count != 0) {
					brandFeature[length / 2 + i]++;
					brandFeature[i] += count;

				} else if (i % 4 == 1 && count != 0) {
					brandFeature[length / 2 + i]++;
					brandFeature[i] += count;

				}else if (i % 4 == 2 && count != 0) {
					brandFeature[length / 2 + i]++;
					brandFeature[i] += count;

				}else if (i % 4 == 3 && count != 0) {
					brandFeature[length / 2 + i]++;
					brandFeature[i] += count;

				}
				
			}
			
			Long[] sumOfTypes = getSumOfTypes(features);
			// 在确定flag和user_id的前提下，肯定没有重复的brand_id
			brandFeatureMap.put(user, sumOfTypes);
		}

		// user brand pair的累加特征:
		// f1_0_add, f1_1_add, ..., f12_3_add

		// user的特征有:(21维)
		// 各个操作的总次数；brand的数目；平均对每个brand操作的次数；各个操作的方差；
		// 发生购买的平均点击次数；发生收藏的平均点击次数；发生购物车的平均点击次数；发生购买的平均收藏次数；发生购买的平均购物车次数；
		// 表结构及字段:
		// flag,user_id,
		// user_click:bigint,user_click_brand_num:bigint,user_click_avg:double,user_click_var:double,
		// user_buy:bigint,user_buy_brand_num:bigint,user_buy_avg:double,user_buy_var:double,
		// user_favorite:bigint,user_favorite_brand_num:bigint,user_favorite_avg:double,user_favorite_var:double,
		// user_cart:bigint,user_cart_brand_num:bigint,user_cart_avg:double,user_cart_val:double,
		// user_click_avg_with_buy:double,user_click_avg_with_favorite:double
		// user_click_avg_with_cart:double,user_favorite_avg_with_buy:double
		// user_cart_avg_with_buy:double
		
//		brandFeature = getBrandFeature(brandFeatureMap);

		result.set(0, key.getBigint("flag"));
		double timeWindow = 124 / (double)(pairFeatureLength);
		if(key.getBigint("flag") == 1) {	//train
			timeWindow = 99 / (double)(pairFeatureLength);
		}
		result.set(1, key.getString("brand_id"));
		Iterator<Entry<String, Long[]>> iter = brandFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			result.set(2, entry.getKey());
			// // 48维累加
			// for (int i = 0; i < featuresAddMap.get(entry.getKey()).size();
			// ++i) {
			// result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i));
			// }
			//
			// // 21(8 + 8 + 3)维user feature
			// // TODO:
			// for (int i = 0; i < 8; ++i) {
			// result.set(i + 51, userFeatureLong[i]);
			// }
			// for (int i = 0; i < 8; ++i) {
			// result.set(i + 59, userFeatureDouble[i]);
			// }
			//
			// for (int i = 0; i < 3; ++i) {
			// result.set(67 + i, advFeature[i]);
			//
			// }
			// // for(int i = 0; i < 3; ++i){
			// // 分母出现0的情况，如何处理
			// result.set(70 + 0, userFeatureLong[1] == 0 ? 3462.0
			// : (double) advFeature[0] / userFeatureLong[1]);
			// result.set(70 + 1, userFeatureLong[1] == 0 ? 56.0
			// : (double) advFeature[1] / userFeatureLong[1]);
			// result.set(70 + 2, userFeatureLong[1] == 0 ? 50.0
			// : (double) advFeature[2] / userFeatureLong[1]);
			// }

			// 16(8 => 68 + 4 + 4)维user feature
			// TODO:
//			for (int i = 0; i < 8; ++i) {
//				result.set(i + 3, brandFeature[i] / (timeWindow * 16.0));
//			}
			for(int i = 0; i < brandFeature.length; ++i){
				if(i < brandFeature.length / 2 - 2){
					if(i % 2 == 0) { //点击
						result.set(i + 3, brandFeature[i] / (timeWindow * (i / 2 + 1)));
					}else {
						result.set(i + 3, brandFeature[i] / (timeWindow * (i / 2 + 1)));
					}
					
				}else if(i >= brandFeature.length / 2 - 2 && i < brandFeature.length / 2){
					result.set(i + 3, brandFeature[i] / (timeWindow * pairFeatureLength));
				}else if(i >= brandFeature.length / 2 && i < brandFeature.length - 2){
					if(i % 2 == 0) {//点击的品牌数
						result.set(i + 3, brandFeature[i] / (timeWindow * ((i-length/2) / 2 + 1)));
					}else {
						result.set(i + 3, brandFeature[i] / (timeWindow * ((i-length/2) / 2 + 1)));
					}
					
				}else{
					result.set(i + 3, brandFeature[i] / (timeWindow * pairFeatureLength));
				}
				
			}
//			------------------------------------------------

			for (int i = 0; i < 4; ++i) {
				if(i == 0) {
					result.set(length + 3 + i, brandAdvFeature[i] / (timeWindow * pairFeatureLength));
				}else {
					result.set(length + 3 + i, brandAdvFeature[i] / (timeWindow * pairFeatureLength));
				}
				

			}
			for (int i = 0; i < 3; ++i) {
				if(i == 0) {
					result.set(length + 3 + 4 + i, (double) (brandAdvFeature[i] + 1.0) / (brandFeature[brandFeature.length/2-3] + 1));
				}else {
					result.set(length + 3 + 4 + i, (double) (brandAdvFeature[i] + 1.0) / (brandFeature[brandFeature.length/2-3] + 1));
				}
				
			}
			// user的平均购买周期如何处理？
			result.set(length + 3 + 4 + 3, (double) (brandAdvFeature[3] + 1.0) / (brandFeature[brandFeature.length/2-3] + 1));
			// 分母出现0的情况，如何处理
//			result.set(70 + 0, userFeatureLong[1] == 0 ? 3462.0
//					: (double) advFeature[0] / userFeatureLong[1]);
//			result.set(70 + 1, userFeatureLong[1] == 0 ? 56.0
//					: (double) advFeature[1] / userFeatureLong[1]);
//			result.set(70 + 2, userFeatureLong[1] == 0 ? 50.0
//					: (double) advFeature[2] / userFeatureLong[1]);

			context.write(result);
		}

	}

	private Long[] initBrandFeature(int length) {
		// TODO Auto-generated method stub
		Long[] result = new Long[length];
		for(int i = 0; i < result.length; ++i){
			result[i] = 0L;
		}
		return result;
	}

	private ArrayList<Long> getPairFeaturesAdd(ArrayList<Long> features) {
		// TODO Auto-generated method stub
		ArrayList<Long> result = new ArrayList<Long>();
		Long[] addOfTypes = new Long[] { 0L, 0L, 0L, 0L };
		for (int i = 0; i < features.size(); ++i) {
			addOfTypes[i % 4] += features.get(i);
			result.add(addOfTypes[i % 4]);
		}
		return result;
	}
	
	private Long[] getBrandFeature(Map<String, Long[]> userFeatureMap) {
		// TODO Auto-generated method stub
		Long[] result = new Long[] { 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L };
		// click,buy,favorite,cart的总数和click,buy,favorite,cart的brand数
		Iterator<Entry<String, Long[]>> iter = userFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			Long[] sumOfBrand = entry.getValue();
			for (int i = 0; i < 4; ++i) {
				result[i] += sumOfBrand[i];
				if (sumOfBrand[i] != 0L) {
					result[4 + i]++;
				}
			}
		}
		return result;
	}

	private Long[] getSumOfTypes(ArrayList<Long> features) {
		// TODO Auto-generated method stub
		Long[] result = new Long[] { 0L, 0L, 0L, 0L, };
		for (int i = 0; i < features.size(); ++i) {
			result[i % 4] += features.get(i);
		}
		return result;
	}
}

package com.guosheng.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

public class CalUserFeatureReduce<E> extends ReducerBase {
	Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		// HashMap来存储品牌及操作
		Map<String, Long[]> userFeatureMap = new HashMap<String, Long[]>();

		 // user brand pair的累加特征，放到这里是为了减少一次map reduce任务
		Map<String, ArrayList<Long>> featuresAddMap = new HashMap<String, ArrayList<Long>>();
		// user brand pair的8个高级特征
		Map<String, Long[]> pairAdvFeatureMap = new HashMap<String, Long[]>();
		
		Long[] userFeatureLong; // user的部分特征，click,buy,favorite,cart的总数和click,buy,favorite,cart的brand数
		Double[] userFeatureDouble; // user的另一部分特征，click,buy,favorite,cart的平均数，方差
		 // 最后一次购买前的点击数，收藏数，购物车数以及time_between_buy在不同brand上的累加
		Long[] userAdvFeature = new Long[] { 0L, 0L, 0L, 0L };

		while (values.hasNext()) {
			Record val = values.next();
			String brand = val.getString("brand_id");
			ArrayList<Long> features = new ArrayList<Long>();
			for (int i = 0; i < 48; ++i) {
				features.add(val.getBigint(i + 1));
			}
			Long[] temp = new Long[]{ 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};
			for(int i = 0; i < temp.length; ++i){
				temp[i] = val.getBigint(49 + i);
			}
			pairAdvFeatureMap.put(brand, temp);
			
			for (int i = 0; i < 3; ++i) {
				userAdvFeature[i] += val.getBigint(49 + i);
			}
			userAdvFeature[3] += val.getBigint("time_between_buy");

			ArrayList<Long> featuresAdd = getPairFeaturesAdd(features);
			featuresAddMap.put(brand, featuresAdd);
			Long[] sumOfTypes = getSumOfTypes(features);
			// 在确定flag和user_id的前提下，肯定没有重复的brand_id
			userFeatureMap.put(brand, sumOfTypes);
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
		userFeatureLong = getUserFeatureLong(userFeatureMap);
		userFeatureDouble = getUserFeatureDouble(userFeatureLong,
				userFeatureMap);

		result.set(0, key.getBigint("flag"));
		result.set(1, key.getString("user_id"));
		Iterator<Entry<String, Long[]>> iter = userFeatureMap.entrySet()
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

			// 测试除以累加时间段的特征
			// 48维累加
			for (int i = 0; i < featuresAddMap.get(entry.getKey()).size(); ++i) {
				result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
						/ (94 / 12.0 * (i / 4 + 1)));
			}
			// user brand pair 的高级特征
			for(int i = 0; i < pairAdvFeatureMap.get(entry.getKey()).length; ++i){
				result.set(51 + i, pairAdvFeatureMap.get(entry.getKey())[i]);
			}
			// 在pair上的发生购买的平均点击次数，平均收藏次数，平均加购物车次数
			// TODO: 计算的方法可能需要再思考一下，0次购买的情况如何处理？
			for(int i = 0; i < 3; ++i){
				result.set(59 + i, (double) (pairAdvFeatureMap.get(entry.getKey())[i] + 1) / (featuresAddMap.get(entry.getKey()).get(45) + 1));
			}
			// pair上的购买周期，即两次购买的平均间隔，0次，1次购买如何处理？
			result.set(62, (double)(pairAdvFeatureMap.get(entry.getKey())[6] + 1) / (featuresAddMap.get(entry.getKey()).get(45) + 1));

			// 24(8 + 8 + 4 + 4)维user feature
			// TODO:
			for (int i = 0; i < 8; ++i) {
				result.set(i + 63, userFeatureLong[i]);
			}
			for (int i = 0; i < 8; ++i) {
				result.set(i + 71, userFeatureDouble[i]);
			}

			for (int i = 0; i < 4; ++i) {
				result.set(79 + i, userAdvFeature[i]);

			}
			for (int i = 0; i < 3; ++i) {
				result.set(83 + i, (double) (userAdvFeature[i] + 1.0) / (userFeatureLong[1] + 1));
			}
			// user的平均购买周期如何处理？
			result.set(86, (double) (userAdvFeature[3] + 1.0) / (userFeatureLong[1] + 1));
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

	private Double[] getUserFeatureDouble(Long[] userFeatureLong,
			Map<String, Long[]> userFeatureMap) {
		// TODO Auto-generated method stub
		Double[] result = new Double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		// click,buy,favorite,cart的平均值和click,buy,favorite,cart的方差
		for (int i = 0; i < 4; ++i) {
			if (userFeatureLong[4 + i] != 0) {
				result[i] = (double) userFeatureLong[i]
						/ userFeatureLong[4 + i];
			} else {
				result[i] = 0.0;
			}
		}
		// 计算方差分子
		Iterator<Entry<String, Long[]>> iter = userFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Long[] values = iter.next().getValue();
			for (int i = 0; i < 4; ++i) {
				result[4 + i] = (values[i] - result[i])
						* (values[i] - result[i]);
			}
		}
		// 计算方差
		for (int i = 0; i < 4; ++i) {
			if (userFeatureLong[4 + i] > 1) {
				result[4 + i] /= (userFeatureLong[4 + i] - 1);
			} else {
				result[4 + i] = 0.0;
			}

		}

		return result;
	}

	private Long[] getUserFeatureLong(Map<String, Long[]> userFeatureMap) {
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

	private Long[] getSumOfTypes(ArrayList<Long> features) {
		// TODO Auto-generated method stub
		Long[] result = new Long[] { 0L, 0L, 0L, 0L, };
		for (int i = 0; i < features.size(); ++i) {
			result[i % 4] += features.get(i);
		}
		return result;
	}

}

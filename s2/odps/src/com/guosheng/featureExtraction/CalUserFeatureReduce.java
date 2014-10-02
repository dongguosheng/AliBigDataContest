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

public class CalUserFeatureReduce extends ReducerBase {
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
		Map<String, Double[]> pairAdvFeatureMap = new HashMap<String, Double[]>();

		Long[] userFeature; // user的部分特征，click,buy,favorite,cart的总数和click,buy,favorite,cart的brand数
		Double[] userAdvFeature = new Double[] { 0.0, 0.0, 0.0, 0.0 };
		int pairFeatureLength = context.getJobConf().getInt(
				"pairFeatureLength", 16);
		int length = (pairFeatureLength * 4) * 2;

		while (values.hasNext()) {
			Record val = values.next();
			String brand = val.getString("brand_id");
			ArrayList<Long> features = new ArrayList<Long>();
			for (int i = 0; i < pairFeatureLength * 4; ++i) {
				features.add(val.getBigint(i + 1));
			}
			Double[] temp = new Double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
					0.0 };
			for (int i = 0; i < temp.length; ++i) {
				temp[i] = val.getDouble(pairFeatureLength * 4 + 1 + i);
			}
			pairAdvFeatureMap.put(brand, temp);

			for (int i = 0; i < 3; ++i) {
				userAdvFeature[i] += val.getDouble(pairFeatureLength * 4 + 1
						+ i);
			}
			userAdvFeature[3] += val.getDouble("time_between_buy");

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

		// userFeature = getUserFeature(userFeatureMap);
		userFeature = getUserFeatureExpand(featuresAddMap, pairFeatureLength);

		result.set(0, key.getBigint("flag"));
		double timeWindow = 124 / (double) (pairFeatureLength);
		if (key.getBigint("flag") == 1) { // train
			timeWindow = 99 / (double) (pairFeatureLength);
		}
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
				if((i + 1) % 4 == 1){ //点击换成取log
					result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
							/ (timeWindow * (i / 4 + 1)));
				}else {
					result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
							/ (timeWindow * (i / 4 + 1)));
				}			
			}
			// user brand pair 的高级特征
			for (int i = 0; i < pairAdvFeatureMap.get(entry.getKey()).length; ++i) {
				if(i == 0 || i == 3){
					result.set(pairFeatureLength * 4 + 3 + i,
							pairAdvFeatureMap.get(entry.getKey())[i]
									/ (timeWindow * pairFeatureLength));
				}else {
					result.set(pairFeatureLength * 4 + 3 + i,
							pairAdvFeatureMap.get(entry.getKey())[i]
									/ (timeWindow * pairFeatureLength));
				}				

			}
			// 在pair上的发生购买的平均点击次数，平均收藏次数，平均加购物车次数
			// TODO: 计算的方法可能需要再思考一下，0次购买的情况如何处理？
			for (int i = 0; i < 3; ++i) {
				result.set(
						pairFeatureLength * 4 + 11 + i,
						(double) (pairAdvFeatureMap.get(entry.getKey())[i] + 1)
								/ (featuresAddMap.get(entry.getKey()).get(
										pairFeatureLength * 4 - 3) + 1));
			}
			// pair上的购买周期，即两次购买的平均间隔，0次，1次购买如何处理？
			result.set(
					pairFeatureLength * 4 + 14,
					(double) (pairAdvFeatureMap.get(entry.getKey())[6] + 1)
							/ (featuresAddMap.get(entry.getKey()).get(
									pairFeatureLength * 4 - 3) + 1));

			// 16(8 => 68 + 4 + 4)维user feature
			// TODO:
			// 8维
//			for (int i = 0; i < 8; ++i) {
//				result.set(i + pairFeatureLength * 4 + 15, userFeature[i]
//						/ (timeWindow * pairFeatureLength));
//			}
			// 68(32+2+32+2)维
			for(int i = 0; i < userFeature.length; ++i){
				if(i < userFeature.length / 2 - 2){
					if(i % 2 == 0) { //点击
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * (i / 2 + 1)));
					}else {
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * (i / 2 + 1)));
					}
					
				}else if(i >= userFeature.length / 2 - 2 && i < userFeature.length / 2){
					result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * pairFeatureLength));
				}else if(i >= userFeature.length / 2 && i < userFeature.length - 2){
					if(i % 2 == 0) {//点击的品牌数
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * ((i-length/2) / 2 + 1)));
					}else {
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * ((i-length/2) / 2 + 1)));
					}
					
				}else{
					result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * pairFeatureLength));
				}
				
			}

			for (int i = 0; i < 4; ++i) {
				if(i == 0){
					result.set(pairFeatureLength * 4 + 15 + length + i, userAdvFeature[i]
							/ (timeWindow * pairFeatureLength));
				}else {
					result.set(pairFeatureLength * 4 + 15 + length + i, userAdvFeature[i]
							/ (timeWindow * pairFeatureLength));
				}
				
			}
			for (int i = 0; i < 3; ++i) {
				if(i == 0){
					result.set(pairFeatureLength * 4 + 15 + length + 4 + i,
							(double) (userAdvFeature[i] + 1.0)
									/ (userFeature[userFeature.length/2 - 3] + 1));
				}else {
					result.set(pairFeatureLength * 4 + 15 + length + 4 + i,
							(double) (userAdvFeature[i] + 1.0)
									/ (userFeature[userFeature.length/2 - 3] + 1));
				}
				
			}
			// user的平均购买周期如何处理？
			result.set(pairFeatureLength * 4 + 15 + length + 4 + 3,
					(double) (userAdvFeature[3] + 1.0) / (userFeature[userFeature.length/2 - 3] + 1));
			// 分母出现0的情况，如何处理
			// result.set(70 + 0, userFeatureLong[1] == 0 ? 3462.0
			// : (double) advFeature[0] / userFeatureLong[1]);
			// result.set(70 + 1, userFeatureLong[1] == 0 ? 56.0
			// : (double) advFeature[1] / userFeatureLong[1]);
			// result.set(70 + 2, userFeatureLong[1] == 0 ? 50.0
			// : (double) advFeature[2] / userFeatureLong[1]);

			context.write(result);
		}

	}

	private Long[] getUserFeatureExpand(Map<String, ArrayList<Long>> featuresAddMap, int pairFeatureLength) {
		// TODO Auto-generated method stub
		// 在时间上离散化user click, user buy, user click brand num and user buy brand
		// num,并作累加
		// 32+2+32+2
		// user_click_1_add, user_buy_1_add, user_click_2_add, user_buy_2_add, ...
		// user_click_16_add, user_buy_16_add, user_favorite, user_cart
		// user_click_brand_num_1_add, user_buy_brand_num_1_add, ...
		// user_favorite_brand_num, user_cart_brand_num
		int length = (pairFeatureLength * 4) * 2;
		Long[] result = new Long[length];
		for(int i = 0; i < result.length; ++i){
			result[i] = 0L;
		}
		Iterator<Entry<String, ArrayList<Long>>> iter = featuresAddMap
				.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, ArrayList<Long>> entry = iter.next();
			ArrayList<Long> value = entry.getValue();
			for (int i = 0; i < value.size(); ++i) {
				Long count = value.get(i);
				if (i % 4 == 0 && count != 0) {	// 
					result[length / 2 + i]++;
					result[i] += count;

				} else if (i % 4 == 1 && count != 0) {
					result[length / 2 + i]++;
					result[i] += count;

				} else if (i % 4 == 2 && count != 0) {
					result[length / 2 + i]++;
					result[i] += count;

				} else if (i % 4 == 3 && count != 0) {
					result[length / 2 + i]++;
					result[i] += count;

				}
			}
		}
		return result;
	}

	private Long[] getUserFeature(Map<String, Long[]> userFeatureMap) {
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

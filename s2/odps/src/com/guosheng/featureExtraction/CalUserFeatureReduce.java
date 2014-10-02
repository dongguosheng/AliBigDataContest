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

		// HashMap���洢Ʒ�Ƽ�����
		Map<String, Long[]> userFeatureMap = new HashMap<String, Long[]>();

		// user brand pair���ۼ��������ŵ�������Ϊ�˼���һ��map reduce����
		Map<String, ArrayList<Long>> featuresAddMap = new HashMap<String, ArrayList<Long>>();
		// user brand pair��8���߼�����
		Map<String, Double[]> pairAdvFeatureMap = new HashMap<String, Double[]>();

		Long[] userFeature; // user�Ĳ���������click,buy,favorite,cart��������click,buy,favorite,cart��brand��
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
			// ��ȷ��flag��user_id��ǰ���£��϶�û���ظ���brand_id
			userFeatureMap.put(brand, sumOfTypes);
		}

		// user brand pair���ۼ�����:
		// f1_0_add, f1_1_add, ..., f12_3_add

		// user��������:(21ά)
		// �����������ܴ�����brand����Ŀ��ƽ����ÿ��brand�����Ĵ��������������ķ��
		// ���������ƽ����������������ղص�ƽ������������������ﳵ��ƽ��������������������ƽ���ղش��������������ƽ�����ﳵ������
		// ��ṹ���ֶ�:
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
			// // 48ά�ۼ�
			// for (int i = 0; i < featuresAddMap.get(entry.getKey()).size();
			// ++i) {
			// result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i));
			// }
			//
			// // 21(8 + 8 + 3)άuser feature
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
			// // ��ĸ����0���������δ���
			// result.set(70 + 0, userFeatureLong[1] == 0 ? 3462.0
			// : (double) advFeature[0] / userFeatureLong[1]);
			// result.set(70 + 1, userFeatureLong[1] == 0 ? 56.0
			// : (double) advFeature[1] / userFeatureLong[1]);
			// result.set(70 + 2, userFeatureLong[1] == 0 ? 50.0
			// : (double) advFeature[2] / userFeatureLong[1]);
			// }

			// ���Գ����ۼ�ʱ��ε�����
			// 48ά�ۼ�
			for (int i = 0; i < featuresAddMap.get(entry.getKey()).size(); ++i) {
				if((i + 1) % 4 == 1){ //�������ȡlog
					result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
							/ (timeWindow * (i / 4 + 1)));
				}else {
					result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
							/ (timeWindow * (i / 4 + 1)));
				}			
			}
			// user brand pair �ĸ߼�����
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
			// ��pair�ϵķ��������ƽ�����������ƽ���ղش�����ƽ���ӹ��ﳵ����
			// TODO: ����ķ���������Ҫ��˼��һ�£�0�ι���������δ���
			for (int i = 0; i < 3; ++i) {
				result.set(
						pairFeatureLength * 4 + 11 + i,
						(double) (pairAdvFeatureMap.get(entry.getKey())[i] + 1)
								/ (featuresAddMap.get(entry.getKey()).get(
										pairFeatureLength * 4 - 3) + 1));
			}
			// pair�ϵĹ������ڣ������ι����ƽ�������0�Σ�1�ι�����δ���
			result.set(
					pairFeatureLength * 4 + 14,
					(double) (pairAdvFeatureMap.get(entry.getKey())[6] + 1)
							/ (featuresAddMap.get(entry.getKey()).get(
									pairFeatureLength * 4 - 3) + 1));

			// 16(8 => 68 + 4 + 4)άuser feature
			// TODO:
			// 8ά
//			for (int i = 0; i < 8; ++i) {
//				result.set(i + pairFeatureLength * 4 + 15, userFeature[i]
//						/ (timeWindow * pairFeatureLength));
//			}
			// 68(32+2+32+2)ά
			for(int i = 0; i < userFeature.length; ++i){
				if(i < userFeature.length / 2 - 2){
					if(i % 2 == 0) { //���
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * (i / 2 + 1)));
					}else {
						result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * (i / 2 + 1)));
					}
					
				}else if(i >= userFeature.length / 2 - 2 && i < userFeature.length / 2){
					result.set(i + pairFeatureLength * 4 + 15, userFeature[i] / (timeWindow * pairFeatureLength));
				}else if(i >= userFeature.length / 2 && i < userFeature.length - 2){
					if(i % 2 == 0) {//�����Ʒ����
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
			// user��ƽ������������δ���
			result.set(pairFeatureLength * 4 + 15 + length + 4 + 3,
					(double) (userAdvFeature[3] + 1.0) / (userFeature[userFeature.length/2 - 3] + 1));
			// ��ĸ����0���������δ���
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
		// ��ʱ������ɢ��user click, user buy, user click brand num and user buy brand
		// num,�����ۼ�
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
		// click,buy,favorite,cart��������click,buy,favorite,cart��brand��
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

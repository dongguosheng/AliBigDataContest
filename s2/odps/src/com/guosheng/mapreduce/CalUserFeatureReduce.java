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

		// HashMap���洢Ʒ�Ƽ�����
		Map<String, Long[]> userFeatureMap = new HashMap<String, Long[]>();

		 // user brand pair���ۼ��������ŵ�������Ϊ�˼���һ��map reduce����
		Map<String, ArrayList<Long>> featuresAddMap = new HashMap<String, ArrayList<Long>>();
		// user brand pair��8���߼�����
		Map<String, Long[]> pairAdvFeatureMap = new HashMap<String, Long[]>();
		
		Long[] userFeatureLong; // user�Ĳ���������click,buy,favorite,cart��������click,buy,favorite,cart��brand��
		Double[] userFeatureDouble; // user����һ����������click,buy,favorite,cart��ƽ����������
		 // ���һ�ι���ǰ�ĵ�������ղ��������ﳵ���Լ�time_between_buy�ڲ�ͬbrand�ϵ��ۼ�
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
				result.set(i + 3, featuresAddMap.get(entry.getKey()).get(i)
						/ (94 / 12.0 * (i / 4 + 1)));
			}
			// user brand pair �ĸ߼�����
			for(int i = 0; i < pairAdvFeatureMap.get(entry.getKey()).length; ++i){
				result.set(51 + i, pairAdvFeatureMap.get(entry.getKey())[i]);
			}
			// ��pair�ϵķ��������ƽ�����������ƽ���ղش�����ƽ���ӹ��ﳵ����
			// TODO: ����ķ���������Ҫ��˼��һ�£�0�ι���������δ���
			for(int i = 0; i < 3; ++i){
				result.set(59 + i, (double) (pairAdvFeatureMap.get(entry.getKey())[i] + 1) / (featuresAddMap.get(entry.getKey()).get(45) + 1));
			}
			// pair�ϵĹ������ڣ������ι����ƽ�������0�Σ�1�ι�����δ���
			result.set(62, (double)(pairAdvFeatureMap.get(entry.getKey())[6] + 1) / (featuresAddMap.get(entry.getKey()).get(45) + 1));

			// 24(8 + 8 + 4 + 4)άuser feature
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
			// user��ƽ������������δ���
			result.set(86, (double) (userAdvFeature[3] + 1.0) / (userFeatureLong[1] + 1));
			// ��ĸ����0���������δ���
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
		// click,buy,favorite,cart��ƽ��ֵ��click,buy,favorite,cart�ķ���
		for (int i = 0; i < 4; ++i) {
			if (userFeatureLong[4 + i] != 0) {
				result[i] = (double) userFeatureLong[i]
						/ userFeatureLong[4 + i];
			} else {
				result[i] = 0.0;
			}
		}
		// ���㷽�����
		Iterator<Entry<String, Long[]>> iter = userFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Long[] values = iter.next().getValue();
			for (int i = 0; i < 4; ++i) {
				result[4 + i] = (values[i] - result[i])
						* (values[i] - result[i]);
			}
		}
		// ���㷽��
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

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

public class CalBrandFeatureReduce<E> extends ReducerBase {
	Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		// HashMap���洢��Ʒ������
		Map<String, Long[]> brandFeatureMap = new HashMap<String, Long[]>();

		Long[] brandFeatureLong; // brand�Ĳ�����������click,buy,favorite,cart��������click,��buy,favorite,cart��brand��
		Double[] brandFeatureDouble; // brand����һ������������click,buy,favorite,cart��ƽ����������;����5ά
		// ���һ�ι���ǰ�ĵ�������ղ��������ﳵ���Լ�time_between_buy�ڲ�ͬuser�ϵ��ۼ�
		Long[] brandAdvFeature = new Long[] { 0L, 0L, 0L, 0L };

		while (values.hasNext()) {
			Record val = values.next();
			String user = val.getString("user_id");
			ArrayList<Long> features = new ArrayList<Long>();
			for (int i = 0; i < 48; ++i) {
				features.add(val.getBigint(i + 1));
			}

			for (int i = 0; i < 3; ++i) {
				brandAdvFeature[i] += val.getBigint(49 + i);
			}
			brandAdvFeature[3] += val.getBigint("time_between_buy");

			Long[] sumOfTypes = getSumOfTypes(features);
			// ��ȷ��flag��user_id��ǰ���£��϶�û���ظ���brand_id
			brandFeatureMap.put(user, sumOfTypes);
		}

		// brand��������:(21ά)
		// �����������ܴ�����user����Ŀ��ƽ������ÿ��user�����Ĵ��������������ķ��
		// ���������ƽ����������������ղص�ƽ������������������ﳵ��ƽ��������������������ƽ���ղش��������������ƽ�����ﳵ������
		// ��ṹ���ֶ�:
		// flag,user_id,brand_id
		// brand_click:bigint,brand_click_user_num:bigint,brand_click_avg:double,brand_click_var:double,
		// brand_buy:bigint,brand_buy_user_num:bigint,brand_buy_avg:double,brand_buy_var:double,
		// brand_favorite:bigint,brand_favorite_user_num:bigint,brand_favorite_avg:double,brand_favorite_var:double,
		// brand_cart:bigint,brand_cart_user_num:bigint,brand_cart_avg:double,brand_cart_val:double,
		// brand_click_avg_with_buy:double,brand_click_avg_with_favorite:double
		// brand_click_avg_with_cart:double,brand_favorite_avg_with_buy:double
		// brand_cart_avg_with_buy:double
		brandFeatureLong = getBrandFeatureLong(brandFeatureMap);
		brandFeatureDouble = getBrandFeatureDouble(brandFeatureLong,
				brandFeatureMap);

		result.set(0, key.getBigint("flag"));
		result.set(1, key.getString("brand_id"));
		Iterator<Entry<String, Long[]>> iter = brandFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			result.set(2, entry.getKey());

			// 21άbrand feature
			// TODO:
			for (int i = 0; i < 8; ++i) {
				result.set(i + 3, brandFeatureLong[i]);
			}
			for (int i = 0; i < 8; ++i) {
				result.set(i + 11, brandFeatureDouble[i]);
			}

			for (int i = 0; i < 4; ++i) {
				result.set(19 + i, brandAdvFeature[i]);
			}
			
			for (int i = 0; i < 3; ++i) {
				result.set(23 + i, (double) (brandAdvFeature[i] + 1.0) / (brandFeatureLong[1] + 1));
			}
			result.set(26, (double) (brandAdvFeature[3] + 1.0) / (brandAdvFeature[1] + 1));
			// ��ĸ����0���������δ���
//			result.set(22 + 0, brandFeatureLong[1] == 0 ? 515.0
//					: (double) advFeature[0] / brandFeatureLong[1]);
//			result.set(22 + 1, brandFeatureLong[1] == 0 ? 8.0
//					: (double) advFeature[1] / brandFeatureLong[1]);
//			result.set(22 + 2, brandFeatureLong[1] == 0 ? 5.0
//					: (double) advFeature[2] / brandFeatureLong[1]);

			context.write(result);
		}

	}

	private Double[] getBrandFeatureDouble(Long[] brandFeatureLong,
			Map<String, Long[]> brandFeatureMap) {
		// TODO Auto-generated method stub
		Double[] result = new Double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		// click,buy,favorite,cart��ƽ��ֵ��click,buy,favorite,cart�ķ���
		for (int i = 0; i < 4; ++i) {
			if (brandFeatureLong[4 + i] != 0) {
				result[i] = (double) brandFeatureLong[i]
						/ brandFeatureLong[4 + i];
			} else {
				result[i] = 0.0;
			}
		}
		// ���㷽�����
		Iterator<Entry<String, Long[]>> iter = brandFeatureMap.entrySet()
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
			if (brandFeatureLong[4 + i] > 1) {
				result[4 + i] /= (brandFeatureLong[4 + i] - 1);
			} else {
				result[4 + i] = 0.0;
			}

		}

		return result;
	}

	private Long[] getBrandFeatureLong(Map<String, Long[]> brandFeatureMap) {
		// TODO Auto-generated method stub
		Long[] result = new Long[] { 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L };
		// click,buy,favorite,cart��������click,buy,favorite,cart��user��
		Iterator<Entry<String, Long[]>> iter = brandFeatureMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			Long[] sumOfUser = entry.getValue();
			for (int i = 0; i < 4; ++i) {
				result[i] += sumOfUser[i];
				if (sumOfUser[i] != 0L) {
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

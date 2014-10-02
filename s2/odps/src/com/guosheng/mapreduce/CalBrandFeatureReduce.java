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

		// HashMap来存储商品及操作
		Map<String, Long[]> brandFeatureMap = new HashMap<String, Long[]>();

		Long[] brandFeatureLong; // brand的部分特征，被click,buy,favorite,cart的总数和click,被buy,favorite,cart的brand数
		Double[] brandFeatureDouble; // brand的另一部分特征，被click,buy,favorite,cart的平均数，方差;其他5维
		// 最后一次购买前的点击数，收藏数，购物车数以及time_between_buy在不同user上的累加
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
			// 在确定flag和user_id的前提下，肯定没有重复的brand_id
			brandFeatureMap.put(user, sumOfTypes);
		}

		// brand的特征有:(21维)
		// 各个操作的总次数；user的数目；平均被对每个user操作的次数；各个操作的方差；
		// 发生购买的平均点击次数；发生收藏的平均点击次数；发生购物车的平均点击次数；发生购买的平均收藏次数；发生购买的平均购物车次数；
		// 表结构及字段:
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

			// 21维brand feature
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
			// 分母出现0的情况，如何处理
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
		// click,buy,favorite,cart的平均值和click,buy,favorite,cart的方差
		for (int i = 0; i < 4; ++i) {
			if (brandFeatureLong[4 + i] != 0) {
				result[i] = (double) brandFeatureLong[i]
						/ brandFeatureLong[4 + i];
			} else {
				result[i] = 0.0;
			}
		}
		// 计算方差分子
		Iterator<Entry<String, Long[]>> iter = brandFeatureMap.entrySet()
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
		// click,buy,favorite,cart的总数和click,buy,favorite,cart的user数
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

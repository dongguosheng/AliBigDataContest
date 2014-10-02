package com.guosheng.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

public class MyReduce extends ReducerBase {
	private Record resultTrain;
	private Record resultTest;

	@Override
	public void setup(TaskContext context) throws IOException {
		resultTrain = context.createOutputRecord();
		resultTest = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		// ����user brand pair������(����ѵ�����Ͳ��Լ�)
		/*
		 * f1_0, f1_1, ..., f12_3, ... (fn_type, train
		 * feature��n����2013-07-15��ʱ���;type�����,
		 * ��f3_1��2013-08-15����ĵ�����ʱ��Σ������ܣ���user����brand�Ĵ���) f1_0_add, f1_1_add,
		 * ...,f12_3_add, ... (add���ۼӣ�train feature����2013-07-15�����ʱ����ۼ�)
		 */
		/*
		 * �����Ĵ洢��ʹ��ArrayList<Long[]>�洢 trainFeature, 2013-04-15��2013-07-15
		 * testFeature, 2013-05-15��2013-08-15
		 */
		ArrayList<Long[]> trainFeature = getInitFeature(12);
		ArrayList<Long[]> testFeature = getInitFeature(12);
		Long[] advFeatureTrain;
		Long[] advFeatureTest;
		// �洢user��brand�Ĳ�����ʱ���ϵ����У�ͨ��actionSeq����һЩ�߼��������磺
		// ��������ĵ����click_before_buy;
		// ����favorite�ĵ����click_before_favorite;
		// ����cart�ĵ����click_before_cart;
		// ���һ�ι�����0��2��3�Ĵ���click_after_last_buy, favorite_after_last_buy,
		// cart_after_last_buy
		// ���һ�ι�������ʱ��
		// user���������
		Map<String, Long[]> actionSeqTrain = new TreeMap<String, Long[]>(
				new Comparator<String>() {

					@Override
					public int compare(String arg0, String arg1) {
						// TODO Auto-generated method stub
						return arg1.compareTo(arg0);
					}
				});
		Map<String, Long[]> actionSeqTest = new TreeMap<String, Long[]>(
				new Comparator<String>() {

					@Override
					public int compare(String arg0, String arg1) {
						// TODO Auto-generated method stub
						return arg1.compareTo(arg0);
					}
				});

		while (values.hasNext()) {
			Record var = values.next();
			String dateStr = var.getString("visit_datetime");
			int type = Integer.parseInt(var.getString("type"));

			int index = getTrainRangeNum(dateStr);
			if (index != -1) {
				trainFeature.get(index)[type]++;
			}
			index = getTestRangeNum(dateStr);
			if (index != -1) {
				testFeature.get(index)[type]++;
			}

			if (dateStr.compareTo("07-17") < 0) { // train set
				if (!actionSeqTrain.containsKey(dateStr)) {
					Long[] value = new Long[] { 0L, 0L, 0L, 0L };
					value[type]++;
					actionSeqTrain.put(dateStr, value);
				} else {
					actionSeqTrain.get(dateStr)[type]++;
				}
			}

			if (dateStr.compareTo("05-14") > 0) { // test set
				if (!actionSeqTest.containsKey(dateStr)) {
					Long[] value = new Long[] { 0L, 0L, 0L, 0L };
					value[type]++;
					actionSeqTest.put(dateStr, value);
				} else {
					actionSeqTest.get(dateStr)[type]++;
				}
			}

		}

		advFeatureTrain = getAdvFeature(actionSeqTrain, "07-17");
		advFeatureTest = getAdvFeature(actionSeqTest, "08-16");

		// ������������Ĳ�output����
		if (!isFiltered(trainFeature)) {
			resultTrain.set(0, 1L); // flag 1 ��ʾtrain set
			resultTrain.set(1, key.getString("user_id"));
			resultTrain.set(2, key.getString("brand_id"));
			for (int i = 0; i < 12; ++i) {
				for (int j = 0; j < 4; ++j) {
					resultTrain.set((4 * i) + j + 3, trainFeature.get(i)[j]);
				}
			}

			// TODO
			for(int i = 0; i < advFeatureTrain.length; ++i){
				resultTrain.set(51 + i, advFeatureTrain[i]);
			}

			context.write(resultTrain);
		}
		if (!isFiltered(testFeature)) {
			resultTest.set(0, 0L); // flag 0 ��ʾtest set
			resultTest.set(1, key.getString("user_id"));
			resultTest.set(2, key.getString("brand_id"));
			for (int i = 0; i < 12; ++i) {
				for (int j = 0; j < 4; ++j) {
					resultTest.set((4 * i) + j + 3, testFeature.get(i)[j]);
				}
			}

			// TODO
			for(int i = 0; i < advFeatureTest.length; ++i){
				resultTest.set(51 + i, advFeatureTest[i]);
			}
			
			context.write(resultTest);
		}
	}
	
	private Long[] getAdvFeatureTry(Map<String, Long[]> actionSeq, String base){
		// advFeature: effective_click, effective_favorite, effective_cart
		//			   ineffective_click, ineffective_favorite, ineffective_cart
		//			   time_between_buy, 0�� => 2 * ��ʱ��, 1�� =����ʱ��, 
		//			   time_after_buy, 0�� =����ʱ��
		Long[] advFeature = new Long[] { 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L };
		boolean isFindLastBuy = false;
		Iterator<Entry<String, Long[]>> iter = actionSeq.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			String dateStr = entry.getKey();
			Long[] actionCounts = entry.getValue();

			if (!isFindLastBuy) {
				if (actionCounts[1] == 0) {
					advFeature[3] += actionCounts[0];
					advFeature[4] += actionCounts[2];
					advFeature[5] += actionCounts[3];
				} else {
					// �ҵ������һ�ι���
					isFindLastBuy = true;
					
					advFeature[7] = getTimeDelta(base, dateStr);
					
					advFeature[0] += actionCounts[0];
					advFeature[1] += actionCounts[2];
					advFeature[2] += actionCounts[3];
				}
			}else{
				if(actionCounts[1] > 0){
					advFeature[6] = getTimeDelta(base, dateStr) - advFeature[7];
				}
				advFeature[0] += actionCounts[0];
				advFeature[1] += actionCounts[2];
				advFeature[2] += actionCounts[3];
			}
		}
		
		// ����û�й������ֻ��һ�ε����
		if(advFeature[6] + advFeature[7] == 0){
			advFeature[6] = 94 * 2L;
			advFeature[7] = 94L;
		}
		if(advFeature[6] == 0 && advFeature[7] != 0){
			advFeature[6] = 94L;
		}
		return advFeature;
	}

	private Long[] getAdvFeature(Map<String, Long[]> actionSeq, String base) {
		// TODO Auto-generated method stub
		// advFeature: click_before_buy, favorite_before_buy, cart_before_buy
		//			   click_after_buy, favorite_after_buy, cart_after_buy
		//			   time_between_buy, time_after_buy
		Long[] advFeature = new Long[] { 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L };
		Long[] beforeBuytTemp = new Long[] { 0L, 0L, 0L}; // 0, 2, 3
		Long[] afterBuyTemp = new Long[] { 0L, 0L, 0L }; // 0, 2, 3
		boolean isFindLastBuy = false;
		Iterator<Entry<String, Long[]>> iter = actionSeq.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Long[]> entry = iter.next();
			String dateStr = entry.getKey();
			Long[] actionCounts = entry.getValue();

			if (!isFindLastBuy) {
				if (actionCounts[1] == 0) {
					afterBuyTemp[0] += actionCounts[0];
					afterBuyTemp[1] += actionCounts[2];
					afterBuyTemp[2] += actionCounts[3];
				} else {
					// �ҵ������һ�ι���
					advFeature[3] = afterBuyTemp[0];
					advFeature[4] = afterBuyTemp[1];
					advFeature[5] = afterBuyTemp[2];
					isFindLastBuy = true;
					
					advFeature[7] = getTimeDelta(base, dateStr);
					
					beforeBuytTemp[0] += actionCounts[0];
					beforeBuytTemp[1] += actionCounts[2];
					beforeBuytTemp[2] += actionCounts[3];
				}
			}else{
				if(actionCounts[1] > 0){
					advFeature[6] = getTimeDelta(base, dateStr) - advFeature[7];
				}
				beforeBuytTemp[0] += actionCounts[0];
				beforeBuytTemp[1] += actionCounts[2];
				beforeBuytTemp[2] += actionCounts[3];
			}
		}
		for(int i = 0; i < 3; ++i){
			advFeature[i] += beforeBuytTemp[i];
		}
		return advFeature;
	}

	private Long getTimeDelta(String base, String dateStr) {
		// TODO Auto-generated method stub
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date dayNow, dayBase;
		long dayDelta = 0;
		try {
			dayBase = df.parse("2013-" + base);
			dayNow = df.parse("2013-" + dateStr);
			dayDelta = TimeUnit.DAYS.convert(
					dayBase.getTime() - dayNow.getTime(),
					TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dayDelta;
	}

	private boolean isFiltered(ArrayList<Long[]> feature) {
		int threshold = 400;
		// ���ȫΪ0�������
		long sum = 0;
		// �޹���ֻ��� 4���µ����������threshold�������
		long sumBuy = 0;
		long sumClick = 0;
		for (Long[] types : feature) {
			for (int i = 0; i < types.length; ++i) {
				sum += types[i];
				if (i == 1) {
					sumBuy += types[i];
				}
				if (i == 0) {
					sumClick += types[i];
				}
			}
		}
		if (sum == 0 || (sumBuy == 0 && sumClick > threshold)) {
			return true;
		} else {
			return false;
		}
	}

	private int getTestRangeNum(String dateStr) {
		// TODO Auto-generated method stub
		double timeWindow = 94 / 12.0; // 2013-05-15��2013-08-16
		Calendar base = Calendar.getInstance();
		base.set(2013, 7, 17); // ����BaseΪ2013-08-16, Java��Calendar��Υ��
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date day;
		long dayDelta = 0;
		try {
			day = df.parse("2013-" + dateStr);
			dayDelta = TimeUnit.DAYS.convert(
					base.getTimeInMillis() - day.getTime(),
					TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (dayDelta > 93) {
			return -1;
		} else {
			return (int) (dayDelta / timeWindow);
		}
	}

	private int getTrainRangeNum(String dateStr) {
		// TODO Auto-generated method stub
		double timeWindow = 94 / 12.0; // 2013-04-15��2013-07-17
		Calendar base = Calendar.getInstance();
		base.set(2013, 6, 17); // ����BaseΪ2013-07-17, Java��Calendar��Υ��
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date day;
		long dayDelta = 0;
		try {
			day = df.parse("2013-" + dateStr);
			dayDelta = TimeUnit.DAYS.convert(
					base.getTimeInMillis() - day.getTime(),
					TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (dayDelta <= 0) {
			return -1;
		} else {
			return (int) (dayDelta / timeWindow);
		}
	}

	private ArrayList<Long[]> getInitFeature(int timeRangeTotal) {
		ArrayList<Long[]> feature = new ArrayList<Long[]>();
		for (int i = 0; i < timeRangeTotal; ++i) {
			feature.add(new Long[] { 0L, 0L, 0L, 0L });
		}
		return feature;
	}

}

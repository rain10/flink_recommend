package top.zhaoyuliu.recommend.business.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;
import top.zhaoyuliu.recommend.core.model.Cooccurrence;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: MatrixReduce.java
 * @Prject: flink-batch-movie
 * @Package: org.arain.flink.batch.movie.reduce
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月13日 上午10:45:59
 * @version: V1.0
 */
public class MatrixReduce implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, String>> out)
			throws Exception {
		Map<String, List<Cooccurrence>> mapC = new HashMap<String, List<Cooccurrence>>();
//		Map<String, Integer> mapA = new HashMap<String, Integer>(); // 和该物品（key中的itemID）同现的其他物品的同现集合。其他物品ID为map的key，同现数字为值
		Map<String, Float> mapB = new HashMap<String, Float>(); // 该物品（key中的itemID），所有用户的推荐权重分数。
		List<Tuple2<String, String>> mapD = new ArrayList<>();
		for (Tuple2<String, String> val : values) {
			String f1 = val.f1;
			if (f1.startsWith("A:")) {
				String[] kv = Pattern.compile("[\t,]").split(f1.substring(2));
				try {
					List<Cooccurrence> list = null;
					if (mapC.containsKey(val.f0)) {
						list = mapC.get(val.f0);
					} else {
						list = new ArrayList<>();
					}
					Cooccurrence coo = new Cooccurrence(val.f0, kv[0], Integer.parseInt(kv[1]));
					list.add(coo);
					mapC.put(val.f0, list);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				mapD.add(val);
			}
		}
		
		for (Tuple2<String, String> val : mapD) {
			String[] kv = Pattern.compile("[\t,]").split(val.f1.substring(2));
			Iterator<Cooccurrence> iterator = mapC.get(val.f0).iterator();
			while(iterator.hasNext()) {
				Cooccurrence co = iterator.next();
				Double result = co.getNum() * Double.parseDouble(kv[1]); // 矩阵乘法相乘计算（只是一一相乘，还未相加）
				out.collect(new Tuple2<String, String>(kv[0], co.getItemID2() + Constants.semicolon + result));
			}
//			try {
//				mapB.put(kv[0], Float.parseFloat(kv[1]));
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
		}
//		
//		Iterator<String> iter = mapC.keySet().iterator();
//		while (iter.hasNext()) {
//			String mapk = iter.next();// itemID
//			Iterator<Cooccurrence> iterator = mapC.get(mapk).iterator();
//			while (iterator.hasNext()) {
//				Cooccurrence co = iterator.next();
//				Iterator<String> iterb = mapB.keySet().iterator();
//				while (iterb.hasNext()) {
//					String mapkb = iterb.next();// userID
//					Float pref = mapB.get(mapkb);
//					Float result = co.getNum() * pref; // 矩阵乘法相乘计算（只是一一相乘，还未相加）
//					out.collect(new Tuple2<String, String>(mapkb, mapk + Constants.semicolon + result));
//				}
//			}
//		}
	}

}

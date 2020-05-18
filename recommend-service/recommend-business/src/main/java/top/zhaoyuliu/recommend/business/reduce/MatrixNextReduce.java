package top.zhaoyuliu.recommend.business.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import top.zhaoyuliu.recommend.core.constant.Constants;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: MatrixNextReduce.java
 * @Prject: flink-batch-movie
 * @Package: org.arain.flink.batch.movie.reduce
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月13日 上午11:10:06
 * @version: V1.0
 */
public class MatrixNextReduce implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Double>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple3<String, String, Double>> out)
			throws Exception {
		Map<String, Map<String, Double>> usermap = new HashMap<String, Map<String, Double>>();// 用户
		for (Tuple2<String, String> val : values) {
			String userId = val.f0;
			Map<String, Double> map = usermap.get(userId);
			String con = val.f1;
			String[] tokens = StringUtils.split(con, Constants.semicolon);
			String itemID = tokens[0];
			Double score = Double.parseDouble(tokens[1]);
			if (null == map) {
				map = new HashMap<>();
				map.put(itemID, score);
			} else {
				if (map.containsKey(itemID)) {
					map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
				} else {
					map.put(itemID, score);
				}
			}
			usermap.put(userId, map);

		}
		List<Entity> list = new ArrayList<Entity>();
		Db.use().execute("delete from recommend_result");
		Iterator<String> iter = usermap.keySet().iterator();
		while (iter.hasNext()) {
			String userId = iter.next();
			Map<String, Double> item = usermap.get(userId);
			for (Entry<String, Double> it : item.entrySet()) {
				out.collect(new Tuple3<String, String, Double>(userId, it.getKey(), it.getValue()));
				Entity e = Entity.create("recommend_result").set("userId", userId).set("itemId", it.getKey())
						.set("rank", it.getValue());
				list.add(e);
			}
		}
		Db.use().insert(list);
	}
}

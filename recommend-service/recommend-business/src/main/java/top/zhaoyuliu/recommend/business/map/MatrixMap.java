package top.zhaoyuliu.recommend.business.map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: MatrixMap.java
 * @Prject: flink-batch-movie
 * @Package: org.arain.flink.batch.movie.map
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月13日 上午10:30:39
 * @version: V1.0
 */
public class MatrixMap implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<String, String>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple3<String, String, String> value, Collector<Tuple2<String, String>> out) throws Exception {
		String f0 = value.f0;
		String f1 = value.f1;
		String f2 = value.f2;
		if (Constants.user.equals(f0)) {
			String[] strings = StringUtils.split(f2, Constants.semicolon);
			// 用户矩阵
			for (String string : strings) {
				String[] vector = StringUtils.split(string, Constants.colon);
				String itemID = vector[0]; // 物品id
				String pref = vector[1]; // 喜爱分数
				out.collect(new Tuple2<String, String>(itemID, "B:" + f1 + Constants.semicolon + pref));
			}
		}
		if (Constants.item.equals(f0)) {
			// 同现矩阵
			String[] v1 = StringUtils.split(f1, Constants.colon);
			String itemID1 = v1[0];
			String itemID2 = v1[1];
			String num = f2;
			out.collect(new Tuple2<String, String>(itemID1, "A:" + itemID2 + Constants.semicolon + num));
		}
	}

}

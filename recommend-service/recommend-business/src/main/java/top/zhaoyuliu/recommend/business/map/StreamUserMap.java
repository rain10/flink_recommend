package top.zhaoyuliu.recommend.business.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: StreamUserMap.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.map
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午4:22:06
 * @version: V1.0
 */
public class StreamUserMap implements FlatMapFunction<Rating, Tuple3<String, String, String>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Rating value, Collector<Tuple3<String, String, String>> out) throws Exception {
		out.collect(new Tuple3<String, String, String>(value.getUserId(),value.getItemId(),value.getRating().toString()));
	}

}

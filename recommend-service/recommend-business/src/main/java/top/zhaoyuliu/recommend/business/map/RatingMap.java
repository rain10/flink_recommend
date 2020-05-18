package top.zhaoyuliu.recommend.business.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.model.Rating;

/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: RatingMap.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.map
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月14日 下午2:04:09
 * @version: V1.0  
 */
public class RatingMap implements FlatMapFunction<Tuple3<String,String,Double>, Rating>{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple3<String, String, Double> value, Collector<Rating> out) throws Exception {
		Rating rating = new Rating(value.f0, value.f1, value.f2);
		out.collect(rating);
	}

}

package top.zhaoyuliu.recommend.business.map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;
import top.zhaoyuliu.recommend.core.model.Rating;
import top.zhaoyuliu.recommend.core.model.Recommend;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: StreamFrontMap.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.map
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午3:36:32
 * @version: V1.0
 */
public class StreamFrontMap implements FlatMapFunction<Recommend, Rating> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Recommend value, Collector<Rating> out) throws Exception {
		String[] split = StringUtils.split(value.getUserContet(), Constants.colon);
		out.collect(new Rating(split[0], split[1], value.getCount()));
	}
}

package top.zhaoyuliu.recommend.business.map;

/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: ItemMap.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.map
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月14日 下午2:36:06
 * @version: V1.0  
 */

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;

public class ItemMap implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, Integer>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, Integer>> out)
			throws Exception {
		String[] split = StringUtils.split(value.f2, Constants.semicolon);
		for (int i = 0; i < split.length; i++) {
			for (int j = 0; j < split.length; j++) {
				out.collect(new Tuple3<String, String, Integer>(Constants.item,
						StringUtils.split(split[i], Constants.colon)[0] + Constants.colon
								+ StringUtils.split(split[j], Constants.colon)[0],
						1));
			}
		}
	}

}

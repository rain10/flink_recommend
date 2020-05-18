package top.zhaoyuliu.recommend.business.map;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;
import top.zhaoyuliu.recommend.core.model.Metrics;
import top.zhaoyuliu.recommend.core.model.Recommend;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: RecommendMap.java
 * @Prject: flink-stream-content
 * @Package: org.arain.flink.stream.content.mapper
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月13日 下午5:29:24
 * @version: V1.0
 */
public class RecommendMap implements FlatMapFunction<Metrics, Recommend> {
	
	private static Logger log = LoggerFactory.getLogger(RecommendMap.class);
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Metrics value, Collector<Recommend> out) throws Exception {
		log.info("开始整理数据 -> {}",value.getTimestamp());
		Map<String, Object> fields = value.getFields();
		Recommend recommend = new Recommend(fields.get("userId") + Constants.colon + fields.get("contentId"), 1.0);
		out.collect(recommend);
	}

}

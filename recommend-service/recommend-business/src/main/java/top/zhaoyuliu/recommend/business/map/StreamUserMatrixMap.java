package top.zhaoyuliu.recommend.business.map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: StreamUserMatrixMap.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.map
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午5:12:19
 * @version: V1.0
 */
public class StreamUserMatrixMap
		implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {
//	private static Logger log = LoggerFactory.getLogger(StreamUserMatrixMap.class);
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple3<String, String, String> value, Collector<Tuple3<String, String, String>> out)
			throws Exception {
		String f1 = value.f1;
		String f2 = value.f2;
		String[] items = StringUtils.split(f1, Constants.semicolon);
		String[] ranks = StringUtils.split(f2, Constants.semicolon);
		StringBuffer buffer = new StringBuffer();
//		log.info("items -> {}", items.length);
//		log.info("ranks -> {}", ranks.length);
		for (int i = 0; i < ranks.length; i++) {
			buffer.append(items[i]).append(Constants.colon).append(ranks[i]).append(Constants.semicolon);
		}
		out.collect(new Tuple3<String, String, String>(Constants.user, value.f0, buffer.toString()));
	}

}

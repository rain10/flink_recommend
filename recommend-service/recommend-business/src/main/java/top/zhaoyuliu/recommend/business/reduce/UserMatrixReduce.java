package top.zhaoyuliu.recommend.business.reduce;

import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.constant.Constants;
import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: UserMatrixReduce.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.reduce
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午2:11:13
 * @version: V1.0
 */
public class UserMatrixReduce implements GroupReduceFunction<Rating, Tuple3<String, String, String>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Rating> values, Collector<Tuple3<String, String, String>> out) throws Exception {
		Tuple3<String, String, String> data = new Tuple3<String, String, String>();
		data.setField(Constants.user, 0);
		Iterator<Rating> iterator = values.iterator();
		StringBuffer buffer = new StringBuffer();
		while (iterator.hasNext()) {
			Rating next = iterator.next();
			buffer.append(next.getItemId()).append(Constants.colon).append(next.getRating()).append(Constants.semicolon);
			data.setField(next.getUserId(), 1);
		}
		data.setField(buffer.toString(), 2);
		out.collect(data);
	}

}

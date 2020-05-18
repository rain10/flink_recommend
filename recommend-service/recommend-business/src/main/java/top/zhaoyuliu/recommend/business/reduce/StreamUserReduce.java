package top.zhaoyuliu.recommend.business.reduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import top.zhaoyuliu.recommend.core.constant.Constants;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: StreamUserReduce.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.reduce
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午4:26:55
 * @version: V1.0
 */
public class StreamUserReduce implements ReduceFunction<Tuple3<String, String, String>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple3<String, String, String> reduce(Tuple3<String, String, String> value1,
			Tuple3<String, String, String> value2) throws Exception {
		String item1 = value1.f1;
		String item2 = value2.f1;
		// 101:3,102:4,
		// 101
		// 3
		String r1 = value1.f2;
		String r2 = value2.f2;
		StringBuffer buffer1 = new StringBuffer();
		StringBuffer buffer2 = new StringBuffer();
		buffer1.append(item1).append(Constants.semicolon).append(item2);
		buffer2.append(r1).append(Constants.semicolon).append(r2);
		Tuple3<String, String, String> data = new Tuple3<String, String, String>(value1.f0, buffer1.toString(),
				buffer2.toString());
		return data;
	}

	public static void main(String[] args) {
		String a = "101:3";
		String[] split = StringUtils.split(a, Constants.semicolon);
		System.out.println(split.length);
	}

}

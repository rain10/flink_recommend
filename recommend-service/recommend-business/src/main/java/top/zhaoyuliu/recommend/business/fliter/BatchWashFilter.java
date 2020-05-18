package top.zhaoyuliu.recommend.business.fliter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: BatchWashFilter.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.fliter
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午2:00:39
 * @version: V1.0
 */
public class BatchWashFilter implements FilterFunction<Tuple3<String, String, Double>> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Tuple3<String, String, Double> value) throws Exception {
		if (StrUtil.isNotBlank(value.f0) && StrUtil.isNotBlank(value.f1) && ObjectUtil.isNotNull(value.f2)) {
			return true;
		}
		return false;
	}

}

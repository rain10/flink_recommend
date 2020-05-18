package top.zhaoyuliu.recommend.business.fliter;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;

import cn.hutool.core.util.ObjectUtil;
import top.zhaoyuliu.recommend.core.model.Metrics;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: WashFilter.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.fliter
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午1:15:40
 * @version: V1.0
 */
public class WashFilter implements FilterFunction<Metrics> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Metrics value) throws Exception {
//		Map<String, Object> data = value.getFields();
//		if (ObjectUtil.isNotNull(value) && ObjectUtil.isNotNull(data.get("userId"))
//				&& ObjectUtil.isNotNull(data.get("itemId")) && ObjectUtil.isNotNull(data.get("rank"))) {
//			return true;
//		}
		return true;
	}

}

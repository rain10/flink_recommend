package top.zhaoyuliu.recommend.business.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import cn.hutool.core.util.StrUtil;
import top.zhaoyuliu.recommend.common.utils.HbaseUtil;
import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: UserItemHbase.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.sink
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月15日 下午1:39:59
 * @version: V1.0
 */
public class UserItemHbase implements SinkFunction<Rating> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(Rating value) throws Exception {
		HbaseUtil.getHbaseConnection();
		HbaseUtil.createTable2("user_item_rank", "info");
		String count = HbaseUtil.getValue("user_item_rank", value.getUserId(), "info", value.getItemId());
		if (StrUtil.isNotBlank(count)) {
			value.setRating(value.getRating() + Double.parseDouble(count));
		}
		HbaseUtil.addOneRecord("user_item_rank", value.getUserId(), "info", value.getItemId(),
				value.getRating().toString());
	}

}

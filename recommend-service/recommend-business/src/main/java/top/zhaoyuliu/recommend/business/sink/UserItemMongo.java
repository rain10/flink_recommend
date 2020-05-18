package top.zhaoyuliu.recommend.business.sink;

import java.util.HashMap;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

import cn.hutool.core.util.ObjectUtil;
import top.zhaoyuliu.recommend.common.utils.MongoUtils;
import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: UserItemMongo.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.sink
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月15日 下午1:40:10
 * @version: V1.0
 */
public class UserItemMongo implements SinkFunction<Rating> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(Rating value) throws Exception {
		HashMap<String, Object> hashMap = new HashMap<>();
		hashMap.put("userId", value.getUserId());
		hashMap.put("itemId", value.getItemId());
		Document document = MongoUtils.findoneby("user_item_rank", "recommend", hashMap);
		if (ObjectUtil.isNotNull(document)) {
			value.setRating(value.getRating() + document.getDouble("rank"));
		}else {
			document = new Document();
			document.put("userId", value.getUserId());
			document.put("itemId", value.getItemId());
		}
		document.put("rank", value.getRating());
		MongoUtils.saveorupdatemongo("user_item_rank", "recommend", document);
	}

}

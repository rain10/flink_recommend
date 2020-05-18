package top.zhaoyuliu.recommend.business.sink;

import java.util.List;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: UserItemMysql.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.sink
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月15日 下午5:32:09
 * @version: V1.0
 */
public class UserItemMysql implements SinkFunction<Rating> {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(Rating value) throws Exception {
		List<Entity> list = Db.use().find(
				Entity.create("user_item_rank").set("userId", value.getUserId()).set("itemId", value.getItemId()));
		if (CollectionUtil.isNotEmpty(list)) {
			Entity entity = list.get(0);
			value.setRating(value.getRating() + entity.getDouble("rank"));
			Db.use().update(Entity.create().set("rank", value.getRating()), // 修改的数据
					Entity.create("user_item_rank").set("userId", value.getUserId()).set("itemId", value.getItemId()) // where条件
			);

		} else {
			Db.use().insert(Entity.create("user_item_rank").set("userId", value.getUserId())
					.set("itemId", value.getItemId()).set("rank", value.getRating()));
		}

	}

}

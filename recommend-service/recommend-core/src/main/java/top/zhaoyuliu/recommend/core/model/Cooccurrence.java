package top.zhaoyuliu.recommend.core.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: Cooccurrence.java
 * @Prject: flink-batch-movie
 * @Package: org.arain.flink.batch.movie.model
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月13日 上午9:41:43
 * @version: V1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Cooccurrence implements Serializable {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	private String itemID1;
	private String itemID2;
	private Integer num;
}

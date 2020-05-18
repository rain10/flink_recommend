package top.zhaoyuliu.recommend.core.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: Rating.java
 * @Prject: flink-batch-movie
 * @Package: org.arain.flink.batch.movie.model
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月12日 上午9:24:06
 * @version: V1.0  
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Rating implements Serializable{
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	private String userId;
	
	private String itemId;
	
	private Double rating;
	
}

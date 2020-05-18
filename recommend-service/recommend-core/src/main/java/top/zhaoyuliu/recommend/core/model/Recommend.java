package top.zhaoyuliu.recommend.core.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: Recommend.java
 * @Prject: flink-stream-content
 * @Package: org.arain.flink.stream.content.entity
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月13日 下午5:31:46
 * @version: V1.0  
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Recommend implements Serializable{/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	
	private String userContet;
	
	private Double count;
}

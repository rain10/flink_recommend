package top.zhaoyuliu.recommend.core.model;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Metrics {
	private Long timestamp;
	private Map<String, Object> fields;
}

package top.zhaoyuliu.recommend.common.schemas;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;

import top.zhaoyuliu.recommend.core.model.Metrics;

/**
 * 
 * <一句话功能简述>
 * @Title: MetricSchema.java
 * @Description: <功能详细描述>
 * @author  Arain
 * @date 2020年5月14日下午12:23:41
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public class MetricSchema implements DeserializationSchema<Metrics>, SerializationSchema<Metrics> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4143287970776981880L;
	private static final Gson gson = new Gson();

    @Override
    public Metrics deserialize(byte[] bytes) throws IOException {
    	String data = new String(bytes);
    	Metrics metrics = new Metrics();
    	metrics.setTimestamp(System.currentTimeMillis());
    	if(null != data) {
    		JSONObject parse = (JSONObject) JSON.parse(data);
    		if(null != parse) {
    			metrics.setTimestamp(parse.getLong("createTime"));
    			metrics.setFields(parse);
    		}
    	}
        return metrics;
    }

    @Override
    public boolean isEndOfStream(Metrics metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(Metrics metricEvent) {
        return gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Metrics> getProducedType() {
        return TypeInformation.of(Metrics.class);
    }
}

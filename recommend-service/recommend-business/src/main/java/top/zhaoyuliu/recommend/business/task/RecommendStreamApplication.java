package top.zhaoyuliu.recommend.business.task;

import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import top.zhaoyuliu.recommend.business.fliter.WashFilter;
import top.zhaoyuliu.recommend.business.map.RecommendMap;
import top.zhaoyuliu.recommend.business.map.StreamFrontMap;
import top.zhaoyuliu.recommend.business.sink.UserItemHbase;
import top.zhaoyuliu.recommend.common.schemas.MetricSchema;
import top.zhaoyuliu.recommend.common.utils.ExecutionEnvUtil;
import top.zhaoyuliu.recommend.common.utils.KafkaConfigUtil;
import top.zhaoyuliu.recommend.common.watermarks.MetricWatermark;
import top.zhaoyuliu.recommend.core.model.Metrics;
import top.zhaoyuliu.recommend.core.model.Rating;
import top.zhaoyuliu.recommend.core.model.Recommend;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: RecommendApplication.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午12:04:36
 * @version: V1.0
 */
public class RecommendStreamApplication {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\winutils\\winutils\\hadoop-2.8.2");
		final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
		StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
		Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);
		FlinkKafkaConsumer011<Metrics> consumer = new FlinkKafkaConsumer011<>(parameterTool.get("metrics.topic.user"),
				new MetricSchema(), props);
		consumer.assignTimestampsAndWatermarks(new MetricWatermark());
		DataStream<Metrics> data = env.addSource(consumer);
		SingleOutputStreamOperator<Rating> initData = data.filter(new WashFilter()).flatMap(new RecommendMap())
				.keyBy(new KeySelector<Recommend, String>() {
					/**
					 * serialVersionUID
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String getKey(Recommend value) throws Exception {
						return value.getUserContet();
					}
				}).timeWindow(Time.seconds(parameterTool.getInt("stream.window.time")))
				.reduce(new ReduceFunction<Recommend>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Recommend reduce(Recommend value1, Recommend value2) throws Exception {
						return new Recommend(value1.getUserContet(), value1.getCount() + value2.getCount());
					}
				}).flatMap(new StreamFrontMap());

//		SingleOutputStreamOperator<Tuple3<String, String, String>> reduce = initData
//				.setParallelism(parameterTool.getInt("stream.parallelism")).flatMap(new StreamUserMap()).keyBy(0)
//				.reduce(new StreamUserReduce());
//
//		SingleOutputStreamOperator<Tuple3<String, String, String>> flatMap = reduce.flatMap(new StreamUserMatrixMap());
		initData.addSink(new UserItemHbase());
		initData.print();
		env.execute("推荐系统");
	}

}

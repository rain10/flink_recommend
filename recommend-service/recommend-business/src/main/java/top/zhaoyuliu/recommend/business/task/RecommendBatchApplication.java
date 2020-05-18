package top.zhaoyuliu.recommend.business.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.business.fliter.BatchWashFilter;
import top.zhaoyuliu.recommend.business.front.BatchFront;
import top.zhaoyuliu.recommend.business.map.ItemMap;
import top.zhaoyuliu.recommend.business.map.MatrixMap;
import top.zhaoyuliu.recommend.business.map.RatingMap;
import top.zhaoyuliu.recommend.business.reduce.MatrixNextReduce;
import top.zhaoyuliu.recommend.business.reduce.MatrixReduce;
import top.zhaoyuliu.recommend.business.reduce.UserMatrixReduce;
import top.zhaoyuliu.recommend.common.utils.ExecutionEnvUtil;
import top.zhaoyuliu.recommend.core.model.Rating;

/**
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: RecommendBatchApplication.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business
 * @Description: <功能详细描述>
 * @author: Arain
 * @date: 2020年5月14日 下午1:20:25
 * @version: V1.0
 */
public class RecommendBatchApplication {

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
		ExecutionEnvironment env = ExecutionEnvUtil.prepareSet(parameterTool);
		DataSet<Tuple3<String, String, Double>> data = BatchFront.handleData(parameterTool, env);
		FlatMapOperator<Tuple3<String, String, Double>, Rating> initData = data.filter(new BatchWashFilter())
				.flatMap(new RatingMap());
		// 用户矩阵
		GroupReduceOperator<Rating, Tuple3<String, String, String>> userData = initData
				.setParallelism(parameterTool.getInt("stream.parallelism")).groupBy(new KeySelector<Rating, String>() {
					/**
					 * serialVersionUID
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String getKey(Rating value) throws Exception {
						return value.getUserId();
					}
				}).reduceGroup(new UserMatrixReduce());
		// 内容矩阵
		FlatMapOperator<Tuple3<String, String, Integer>, Tuple3<String, String, String>> itemData = userData
				.flatMap(new ItemMap()).groupBy(1).aggregate(Aggregations.SUM, 2)
				.flatMap(new FlatMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>>() {
					/**
					 * serialVersionUID
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Tuple3<String, String, Integer> value,
							Collector<Tuple3<String, String, String>> out) throws Exception {
						out.collect(new Tuple3<String, String, String>(value.f0, value.f1, value.f2.toString()));
					}
				});
		// 聚合并处理
		GroupReduceOperator<Tuple2<String, String>, Tuple3<String, String, Double>> outData = userData.union(itemData)
				.flatMap(new MatrixMap()).reduceGroup(new MatrixReduce()).reduceGroup(new MatrixNextReduce());
		outData.print();
	}
}

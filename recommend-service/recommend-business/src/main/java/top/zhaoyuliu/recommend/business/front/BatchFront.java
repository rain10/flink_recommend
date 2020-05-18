package top.zhaoyuliu.recommend.business.front;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: BatchFront.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.front
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月14日 下午1:25:43
 * @version: V1.0  
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import top.zhaoyuliu.recommend.common.utils.MongoUtils;
import top.zhaoyuliu.recommend.core.enums.BatchEnum;

public class BatchFront {

	public final static DataSet<Tuple3<String, String, Double>> handleData(ParameterTool parameterTool,
			ExecutionEnvironment env) throws Exception {
		String string = parameterTool.get("batch.type");
		switch (BatchEnum.valueOf(string)) {
		case CSV:
			return csvData(parameterTool, env);
		case MONGODB:
			return mongoData(parameterTool, env);
		case MYSQL:
			return mysqlData(parameterTool, env);
		}
		throw new RuntimeException("批处理类型异常");
	}

	private static DataSet<Tuple3<String, String, Double>> mysqlData(ParameterTool parameterTool,
			ExecutionEnvironment env) throws Exception {
		String table = parameterTool.get("batch.mysql.table");
		if (StrUtil.isBlank(table)) {
			throw new RuntimeException("mysql数据表为空");
		}
		List<Entity> list = Db.use().findAll(Entity.create(table));
		List<Tuple3<String, String, Double>> data = new ArrayList<>();
		list.forEach(item -> {
			data.add(new Tuple3<String, String, Double>(item.getStr("userId"), item.getStr("itemId"),
					item.getDouble("rank")));
		});
		DataSet<Tuple3<String, String, Double>> fromCollection = env.fromCollection(data);
		return fromCollection;
	}

	private static DataSet<Tuple3<String, String, Double>> csvData(ParameterTool parameterTool,
			ExecutionEnvironment env) throws Exception {
		String path = parameterTool.get("batch.csv.path");
		if (StrUtil.isBlank(path)) {
			throw new RuntimeException("文件地址为空");
		}
		String split = parameterTool.get("batch.csv.split");
		boolean head = parameterTool.getBoolean("batch.csv.head");
		CsvReader csvReader = env.readCsvFile(path).parseQuotedStrings('"').ignoreInvalidLines();
		if (head) {
			csvReader.ignoreFirstLine();
		}
		if (StrUtil.isBlank(split)) {
			csvReader.fieldDelimiter(split);
		}
		DataSet<Tuple3<String, String, Double>> data = csvReader.types(String.class, String.class, Double.class);
		return data;
	}

	private static DataSet<Tuple3<String, String, Double>> mongoData(ParameterTool parameterTool,
			ExecutionEnvironment env) throws Exception {
		String host = parameterTool.get("mongoDB.host");
		if (StrUtil.isBlank(host)) {
			throw new RuntimeException("数据库host为空");
		}
		String port = parameterTool.get("mongoDB.port");
		if (StrUtil.isBlank(port)) {
			throw new RuntimeException("数据库port为空");
		}
		String database = parameterTool.get("batch.mongoDB.database");
		if (StrUtil.isBlank(database)) {
			throw new RuntimeException("数据库为空");
		}
		String table = parameterTool.get("batch.mongoDB.table");
		if (StrUtil.isBlank(table)) {
			throw new RuntimeException("数据表为空");
		}
		List<Tuple3<String, String, Double>> data = new ArrayList<>();
		List<Document> list = MongoUtils.findListby(table, database, new HashMap<>());
		list.forEach(item -> {
			data.add(new Tuple3<String, String, Double>(item.getString("userId"), item.getString("itemId"),
					item.getDouble("rank")));
		});
		DataSet<Tuple3<String, String, Double>> fromCollection = env.fromCollection(data);
		return fromCollection;
	}

}

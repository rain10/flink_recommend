package top.zhaoyuliu.recommend.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author Arain
 *
 */
public class MongoUtils {

	private static MongoClient mongoClient = new MongoClient(ExecutionEnvUtil.PARAMETER_TOOL.get("mongoDB.host"),
			Integer.parseInt(ExecutionEnvUtil.PARAMETER_TOOL.get("mongoDB.port")));

	@SuppressWarnings("rawtypes")
	public static Document findoneby(String tablename, String database, Map<String, Object> map) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		MongoCollection mongoCollection = mongoDatabase.getCollection(tablename);
		Document doc = new Document(map);
		@SuppressWarnings("unchecked")
		FindIterable<Document> itrer = mongoCollection.find(doc);
		MongoCursor<Document> mongocursor = itrer.iterator();
		if (mongocursor.hasNext()) {
			return mongocursor.next();
		} else {
			return null;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static List<Document> findListby(String tablename, String database, Map<String, Object> map) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		MongoCollection mongoCollection = mongoDatabase.getCollection(tablename);
		List<Document> list = new ArrayList<>();
		Document doc = new Document(map);
		@SuppressWarnings("unchecked")
		FindIterable<Document> itrer = mongoCollection.find(doc);
		MongoCursor<Document> mongocursor = itrer.iterator();
		while (mongocursor.hasNext()) {
			list.add(mongocursor.next());
		} 
		return list;
	}
	
	@SuppressWarnings("rawtypes")
	public static void remove(String tablename, String database, Map<String, Object> map) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		MongoCollection mongoCollection = mongoDatabase.getCollection(tablename);
		Document doc = new Document(map);
		mongoCollection.deleteMany(doc);
	}

	public static void saveorupdatemongo(String tablename, String database, Document doc) {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		MongoCollection<Document> mongocollection = mongoDatabase.getCollection(tablename);
		if (!doc.containsKey("_id")) {
			ObjectId objectid = new ObjectId();
			doc.put("_id", objectid);
			mongocollection.insertOne(doc);
			return;
		}
		Document matchDocument = new Document();
		String objectid = doc.get("_id").toString();
		matchDocument.put("_id", new ObjectId(objectid));
		FindIterable<Document> findIterable = mongocollection.find(matchDocument);
		if (findIterable.iterator().hasNext()) {
			mongocollection.updateOne(matchDocument, new Document("$set", doc));
			try {
				System.out.println("come into saveorupdatemongo ---- update---" + JSONObject.toJSONString(doc));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			mongocollection.insertOne(doc);
			try {
				System.out.println("come into saveorupdatemongo ---- insert---" + JSONObject.toJSONString(doc));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

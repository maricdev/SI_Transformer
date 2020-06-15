package com.si.transformator.services.implementations;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.si.transformator.services.ITransformatorService;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

@Service
public class TransformatorServiceImpl implements ITransformatorService {

    @Autowired
    private Environment env;
    private Connection connection = null;
    private MongoClient mongoClient = null;
    private MongoDatabase database = null;
    private Date time = null;

    private static void AddValueToField(Object object, HashMap<String, Object> fields) throws JSONException {

        if (object instanceof JSONArray) {
            JSONArray array = (JSONArray) object;
            for (int i = 0; i < array.length(); ++i) AddValueToField(array.get(i), fields);
        } else if (object instanceof JSONObject) {
            JSONObject json = (JSONObject) object;
            JSONArray names = json.names();

            if (names == null) return;
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                for (int i = 0; i < names.length(); ++i) {
                    String fieldKey = entry.getKey();
                    Object fieldValue = entry.getValue();
                    String jsonKey = names.getString(i);
                    //System.out.println("Key: " + fieldKey + ", Value: " + fieldValue);
                    if (json.get(jsonKey).equals(fieldKey)) {
                        if (fieldValue == null)
                            json.put(jsonKey, JSONObject.NULL);
                        else
                            json.put(jsonKey, fieldValue);
                    } else {
                        AddValueToField(json.get(jsonKey), fields);
                    }
                }
            }
        }
    }

    public String TransformModel(String body, Long services_id, Long endpoints_id) throws SQLException, ClassNotFoundException {
        JSONObject json = new JSONObject(body);
        String type = json.getString("type");
        String query = json.getString("query");
        String formatString = json.getJSONObject("format").toString();
        JSONObject index = json.getJSONObject("index");

        Object mongoDb = null;
        if (connection == null || connection.isClosed())
            CreateMySQLConnection();
        if (mongoClient == null)
            CreateMongoConnection();

        if (type.equals("mongo")) {
            String report = "S-" + services_id + "E-" + endpoints_id;
            JSONArray jsonArray;
            JSONObject jsonObject;
            MongoCollection<Document> collection;
            time = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            String strDate = formatter.format(time);

            if (checkIfCollectionExist(report) == false) {
                JSONObject indexFields = index.getJSONObject("fields");
                mongoDb = TransformToMongo(query, formatString);
                CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
                if (index.get("autoindex").equals("false"))
                    createCollectionOptions.autoIndex(false);

                database.createCollection(report, createCollectionOptions);
                collection = database.getCollection(report);

                IndexOptions indexOptions = new IndexOptions();
                if (index.get("unique").equals("true"))
                    indexOptions.unique(true);
                else if (index.get("unique").equals("false"))
                    indexOptions.unique(false);

                HashMap<String, Object> keyIndexs = new Gson().fromJson(indexFields.toString(), HashMap.class);
                System.out.println("KEYS: " + keyIndexs);
                for (Map.Entry<String, Object> entry : keyIndexs.entrySet()) {
                    Integer broj = (int) Double.parseDouble(entry.getValue().toString());
                    keyIndexs.put(entry.getKey(), broj > 0 ? 1 : broj == 0 ? 0 : -1);
                }

                collection.createIndex(new Document(keyIndexs), indexOptions);

//                    indexFields.keySet().forEach(key ->
//                    {
//                        Object value = indexFields.get(key);
//                        System.out.println("key: " + key + " value: " + value);
//                        database.getCollection(report).createIndex(new Document(key, value), indexOptions);
//                    });


                List<Document> dbObjects = new ArrayList<>();

                jsonArray = (JSONArray) mongoDb;
                for (Object object : jsonArray) {
                    JSONObject objekat = (JSONObject) object;
                    objekat.put("timestamp", strDate);
                    dbObjects.add(Document.parse(objekat.toString()));
                }
                collection.insertMany(dbObjects);
            }
            collection = database.getCollection(report);
            FindIterable<Document> documents = collection.find();
            jsonArray = new JSONArray();
            for (Document document : documents) {
                jsonObject = new JSONObject(document.toJson());
                jsonArray.put(jsonObject);
                System.out.println(document.toJson());
            }
            //database.getCollection("").find().sort()

            collection = database.getCollection("REPORTS");
            Document dbObjects = new Document();

            JSONObject obj = new JSONObject();
            obj.put("name", report);

            obj.put("timestamp", strDate);
            dbObjects = Document.parse(obj.toString());
            collection.insertOne(dbObjects);

            return jsonArray.toString();
        } else if (type.equals("arango"))
            TransformToArango(query, formatString);

        System.out.println("Done");
        return null;

    }

    @Override
    public String generateReport(String body, String name) {
        if (mongoClient == null)
            CreateMongoConnection();
        if (checkIfCollectionExist(name)) {
            JSONObject json = new JSONObject(body);
            JSONArray jsonArray = new JSONArray();
            String type = json.getString("type");
            Integer range = json.getInt("range");
            JSONObject filter = json.getJSONObject("filter");
            HashMap<String, Object> filterIndexes = new Gson().fromJson(filter.toString(), HashMap.class);
            MongoCollection<Document> collection = database.getCollection(name);

            FindIterable<Document> documents = collection.find();
            jsonArray = new JSONArray();
            int count = 0;
            for (Document document : documents) {
                boolean exist = true;
                JSONObject jsonObject = new JSONObject(document.toJson());

                for (Map.Entry<String, Object> entry : filterIndexes.entrySet()) {
                    if (jsonObject.has(entry.getKey())) {
                        if (!jsonObject.get(entry.getKey()).equals(entry.getValue())) {
                            exist = false;
                            break;
                        }
                    }
                }

                if (count >= range && range != -1)
                    break;
                if (exist == true) {
                    jsonArray.put(jsonObject);
                    count++;
                }
            }
            List<Document> dbObjects = new ArrayList<>();
            time = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            String strDate = formatter.format(time);
            if (!jsonArray.isEmpty()) {

                if (!checkIfCollectionIsEmpty("REPORT_" + name)) {
                    collection = database.getCollection("REPORT_" + name);
                    BasicDBObject document = new BasicDBObject();
                    collection.deleteMany(document);
                }
                collection = database.getCollection("REPORT_" + name);
                for (Object object : jsonArray) {
                    JSONObject objekat = (JSONObject) object;
                    objekat.put("timestamp_report", strDate);
                    dbObjects.add(Document.parse(objekat.toString()));
                }
                collection.insertMany(dbObjects);

                documents = collection.find();
                jsonArray = new JSONArray();
                for (Document document : documents) {
                    JSONObject jsonObject = new JSONObject(document.toJson());
                    jsonArray.put(jsonObject);
                }
            }

            return jsonArray.toString();
        } else
            return "Gađaš pogrešno sram te bilo bre.";
    }

    private Object TransformToMongo(String query, String formatString) throws SQLException {
        Statement stmt = null;
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();

        try {

            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            ResultSetMetaData metadata = rs.getMetaData();
            List<String> columNames = new ArrayList<>();

            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                if ((metadata.getColumnLabel(i).equals(metadata.getColumnName(i))))
                    columNames.add(metadata.getTableName(i) + "." + metadata.getColumnName(i));
                else
                    columNames.add(metadata.getColumnLabel(i));
            }
            System.out.println(columNames);

            int count = 0;
            while (rs.next()) {
                count++;

                Object temp = new JSONObject(formatString);
                String row = "";

                HashMap<String, Object> fields = new HashMap<>();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    fields.put(columNames.get(i - 1), rs.getString(i));
                    row += rs.getString(i) + ", ";
                }
                AddValueToField(temp, fields);

                jsonObject = (JSONObject) temp;

                if (count > 0 && !jsonObject.isEmpty())
                    jsonArray.put(jsonObject);

            }
        } catch (SQLException e) {
            e.getStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (jsonArray.length() > 0)
                return jsonArray;
            else
                return jsonObject;
        }

    }

    private void TransformToArango(String query, String object) {
    }

    private Boolean checkIfCollectionExist(String coll) {
        for (String name : database.listCollectionNames()) {
            if (name.trim().equals(coll))
                return true;
        }
        return false;

    }

    private Boolean checkIfCollectionIsEmpty(String coll) {
        if (checkIfCollectionExist(coll) == true) {
            MongoCollection<Document> collection = database.getCollection(coll);
            return collection.countDocuments() <= 0;
        } else
            return true;
    }

    private void CreateMySQLConnection() throws ClassNotFoundException, SQLException {

        String driver = env.getProperty("spring.datasource.driver-class-name");
        if (driver != null) {
            Class.forName(driver);
        }

        String url = env.getProperty("spring.datasource.url");
        System.out.println(url);
        String username = env.getProperty("spring.datasource.username");
        String password = env.getProperty("spring.datasource.password");

        connection = DriverManager.getConnection(url, username, password);
    }

    private void CreateMongoConnection() {
        MongoCredential credential = MongoCredential.createCredential(env.getProperty("spring.data.mongodb.username"), env.getProperty("spring.data.mongodb.database"),
                env.getProperty("spring.data.mongodb.password").toCharArray());
        mongoClient = new MongoClient(new ServerAddress(env.getProperty("spring.data.mongodb.host"), Integer.parseInt(env.getProperty("spring.data.mongodb.port"))), Arrays.asList(credential));
        database = mongoClient.getDatabase(env.getProperty("spring.data.mongodb.database"));
        for (String name : database.listCollectionNames()) {
            System.out.println(name);
        }
    }
}

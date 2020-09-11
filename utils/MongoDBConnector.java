package com.ayscom.minetur.utils;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.connection.Server;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


/**
 * Created by Christian on 29/6/15.
 */


public class MongoDBConnector {

    private HashMap<String, MongoDatabase> _dbs;
    private MongoClient _mongoClient;

    static Logger LOG = Logger.getLogger(MongoDBConnector.class);

    private static HashMap<ServerConnection, MongoDBConnector> connectors = new HashMap<>();

    public static MongoDBConnector getConnector(ServerConnection serverConnection) {
        if (connectors.containsKey(serverConnection)) {
            return connectors.get(serverConnection);
        } else {
            MongoDBConnector connector = new MongoDBConnector(serverConnection.getIP(), serverConnection.getPort());
            connectors.put(serverConnection, connector);
            return connector;
        }
    }

    private MongoDBConnector(String ip, int port) {
        try {
            _mongoClient = new MongoClient(ip, port);
            _dbs = new HashMap<>();
            LOG.info("Connect to mongodb server " + ip + ":" + port + " successfully");
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't connect to MongoDB\n" + e.toString());
        }
    }



    public MongoCursor<Document> find(String db_name, String collection_name, Bson where) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            MongoCursor<Document> cursor;
            if (where != null)
                cursor = collection.find(where).iterator();
            else
                cursor = collection.find().iterator();
            LOG.info("Correctly find on MongoDB");
            return cursor;
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't find on MongoDB\n" + e.toString());
        }
    }

    public MongoCursor<Document> findAll(String db_name, String collection_name) {
        return find(db_name, collection_name, null);
    }

    public Document findOne(String db_name, String collection_name, Bson where) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            Document result;
            if (where != null)
                result = (Document)collection.find(where).first();
            else
                result = (Document)collection.find().first();
            LOG.info("Correctly find on MongoDB");
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't find on MongoDB\n" + e.toString());
        }
    }

    public BasicDBList findGroup(String db_name, String collection_name, Bson where) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            BasicDBList result;
            if (where != null)
                result = (BasicDBList)collection.find(where);
            else
                result = (BasicDBList)collection.find();
            LOG.info("Correctly find on MongoDB");
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't find on MongoDB\n" + e.toString());
        }
    }

    public void insert(String db_name, String collection_name, Document document) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            try {
                collection.insertOne(document);
            } catch (MongoWriteException e) {
                LOG.info("Document with ID " + document.get("_id") + " duplicated. Not inserted");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't insert in " + collection_name + " on MongoDB\n" + e.toString());
        }

    }


    public void delete(String db_name, String collection_name, Document document) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            try {
                collection.deleteOne(document);
            } catch (MongoWriteException e) {
                LOG.info("Document with ID " + document.get("_id") + "  Not delete");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't delete in " + collection_name + " on MongoDB\n" + e.toString());
        }

    }

    public void bulkInsert(String db_name, String collection_name, ArrayList<BasicDBObject> documents) {
        MongoDatabase db;
        if (!_dbs.containsKey(db_name)) {
            db = _mongoClient.getDatabase(db_name);
            _dbs.put(db_name, db);
        } else {
            db = _dbs.get(db_name);
        }
        //MongoCollection collection = db.getCollection(collection_name);

        DBCollection collection = (DBCollection) db.getCollection(collection_name);
        BulkWriteOperation  bulkWriteOperation= collection.initializeUnorderedBulkOperation();

        for( BasicDBObject doc: documents){
            bulkWriteOperation.insert(doc);
        }
        BulkWriteResult result = bulkWriteOperation.execute();

    }

    public void bulkDelete(String db_name, String collection_name, ArrayList< BasicDBObject> documents) {
        MongoDatabase db;
        if (!_dbs.containsKey(db_name)) {
            db = _mongoClient.getDatabase(db_name);
            _dbs.put(db_name, db);
        } else {
            db = _dbs.get(db_name);
        }
       // MongoCollection collection = db.getCollection(collection_name);

        DBCollection collection = (DBCollection) db.getCollection(collection_name);
        BulkWriteOperation  bulkWriteOperation= collection.initializeUnorderedBulkOperation();

        for( BasicDBObject doc: documents){
            bulkWriteOperation.find(doc).remove();
        }
        BulkWriteResult result = bulkWriteOperation.execute();


    }

    public void insertOrUpdate(String db_name, String collection_name, Document document) {
        try {
            MongoDatabase db;
            if (!_dbs.containsKey(db_name)) {
                db = _mongoClient.getDatabase(db_name);
                _dbs.put(db_name, db);
            } else {
                db = _dbs.get(db_name);
            }
            MongoCollection collection = db.getCollection(collection_name);
            try {
                collection.insertOne(document);
            } catch (MongoWriteException e) {
                Document search = new Document();
                search.append("_id", document.get("_id"));
                collection.replaceOne(search, document);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error: Can't insert in " + collection_name + " on MongoDB\n" + e.toString());
        }
    }

    public void close() {
        _mongoClient.close();
    }


}

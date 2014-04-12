package org.opencb.datastore.mongodb;

import java.util.*;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by imedina on 22/03/14.
 */
public class MongoDataStore {

    private static Map<String, MongoDBCollection> mongoCollections = new HashMap<>();

    private MongoClient mongoClient;
    private DB db;
    private MongoDBConfiguration mongoDBConfiguration;
    private String database;

    protected Logger logger;

    MongoDataStore(MongoClient mongoClient, DB db, MongoDBConfiguration mongoDBConfiguration) {
        this.mongoClient = mongoClient;
        this.db = db;
        this.mongoDBConfiguration = mongoDBConfiguration;
        this.database = db.getName();
        init();
    }

    private void init() {
        logger = LoggerFactory.getLogger(MongoDataStore.class);
    }

    public MongoDBCollection getCollection(String collection) {
        if(!mongoCollections.containsKey(collection)) {
            MongoDBCollection mongoDBCollection = new MongoDBCollection(db.getCollection(collection));
            mongoCollections.put(collection, mongoDBCollection);
            logger.info("MongoDataStore: new MongoDBCollection created");
        }
        return mongoCollections.get(collection);
    }


    public boolean test() {
        CommandResult commandResult = db.getStats();
        return commandResult != null && commandResult.getBoolean("ok");
    }


    public void close() {
        logger.info("MongoDataStore: connection closed");
        mongoClient.close();
    }

    /*
     *
     * GETTERS, NO SETTERS ARE AVAILABLE TO MAKE THIS CLASS IMMUTABLE
     *
     */

    public static Map<String, MongoDBCollection> getMongoCollections() {
        return mongoCollections;
    }

    public DB getDb() {
        return db;
    }

    public String getDatabaseName() {
        return db.getName();
    }

    public MongoDBConfiguration getMongoDBConfiguration() {
        return mongoDBConfiguration;
    }

}

package org.opencb.datastore.mongodb;

import com.mongodb.*;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by imedina on 22/03/14.
 */
public class MongoDataStore {

    private static Map<String, MongoDBCollection> mongoCollections;
    private List<DataStoreServerAddress> dataStoreServerAddresses;

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

    @Deprecated
    MongoDataStore(String database, MongoDBConfiguration mongoDBConfiguration) {
        this.database = database;
        this.mongoDBConfiguration = mongoDBConfiguration;
    }

    private void init() {
        mongoCollections = new HashMap<>();

        logger = LoggerFactory.getLogger(MongoDataStore.class);
    }

    public MongoDBCollection getCollection(String collection) {
        if(!mongoCollections.containsKey(collection)) {
            logger.info("MongoDataStore: new MongoDBCollection created");
            MongoDBCollection mongoDBCollection = new MongoDBCollection(db.getCollection(collection));
            mongoCollections.put(collection, mongoDBCollection);
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

    /**
     *
     * GETTERS, NO SETTERS ARE AVAILABLE TO MAKE THIS CLASS IMMUTABLE
     *
     **/

    public static Map<String, MongoDBCollection> getMongoCollections() {
        return mongoCollections;
    }

    public DB getDb(String database) {
        return db;
    }

    public String getDatabase() {
        return database;
    }

    public MongoDBConfiguration getMongoDBConfiguration() {
        return mongoDBConfiguration;
    }

}

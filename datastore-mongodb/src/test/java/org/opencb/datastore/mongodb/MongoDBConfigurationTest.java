package org.opencb.datastore.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by imedina on 25/03/14.
 */
public class MongoDBConfigurationTest {

    @Before
    public void setUp() throws Exception {
        MongoDBConfiguration mongoDBConfiguration = new MongoDBConfiguration.Builder().build();
        System.out.println(mongoDBConfiguration);
    }

    @Test
    public void testInit() throws Exception {
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().init()
                .add("writeConcern", "ACK")
                .build();
        System.out.println(mongoDBConfiguration.toJson());

        MongoClient client = new MongoClient("localhost");
        DB db = client.getDB("test");

        MongoDataStore mongoDataStore = new MongoDataStore(client, db, mongoDBConfiguration);
    }
}

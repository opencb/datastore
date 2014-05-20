package org.opencb.datastore.mongodb;

import org.junit.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by imedina on 13/04/14.
 */
public class MongoDataStoreTest {

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("test");
        mongoDataStore.createCollection("JUnitTest");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mongoDataStoreManager.close("test");
    }


    @Test
    public void testTest() throws Exception {
        mongoDataStore.test();
    }

    @Test
    public void testGetCollection() throws Exception {
        mongoDataStore.getCollection("JUnitTest");
    }

    @Test
    public void testCreateCollection() throws Exception {

    }

    @Test
    public void testDropCollection() throws Exception {

    }

    @Test
    public void testGetCollectionNames() throws Exception {
        List<String> colNames = mongoDataStore.getCollectionNames();
        Arrays.toString(colNames.toArray());
    }

    @Test
    public void testGetStats() throws Exception {

    }
}

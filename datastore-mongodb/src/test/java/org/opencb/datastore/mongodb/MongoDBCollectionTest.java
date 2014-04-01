package org.opencb.datastore.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.datastore.core.QueryOptions;

import java.util.Arrays;

/**
 * Created by imedina on 29/03/14.
 */
public class MongoDBCollectionTest {

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;
    private static MongoDBCollection mongoDBCollection;


    @BeforeClass
    public static void setUp() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("test");

        mongoDBCollection = mongoDataStore.getCollection("protein");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mongoDataStore.close();
    }

    @Test
    public void testCount() throws Exception {
        System.out.println(mongoDBCollection.count().getResult());
        System.out.println(mongoDBCollection.nativeQuery().count());
    }

    @Test
    public void testDistinct() throws Exception {
        System.out.println(mongoDBCollection.distinct("name", null).getNumResults());
//        System.out.println(mongoDBCollection.nativeQuery().distinct("name"));
    }

    @Test
    public void testFind() throws Exception {
        DBObject dbObject = new BasicDBObject("name", "STAC_HUMAN");
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("accession"));
        System.out.println(mongoDBCollection.find(dbObject, queryOptions).getResult());
//        System.out.println(mongoDBCollection.nativeQuery().find(dbObject, null));
    }

    @Test
    public void testAggregate() throws Exception {

    }
}

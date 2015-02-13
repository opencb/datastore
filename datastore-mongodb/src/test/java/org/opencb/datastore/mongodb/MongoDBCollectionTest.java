package org.opencb.datastore.mongodb;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.*;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.QueryResultWriter;

import static org.junit.Assert.*;

/**
 * Created by imedina on 29/03/14.
 */
public class MongoDBCollectionTest {

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;
    private static MongoDBCollection mongoDBCollection;
    private static MongoDBCollection mongoDBCollectionInsertTest;
    private static MongoDBCollection mongoDBCollectionRemoveTest;

    private static int N = 1000;

    @BeforeClass
    public static void beforeClass() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("datastore_test");

        mongoDBCollection = createTestCollection("test", N);
        mongoDBCollectionInsertTest = createTestCollection("insert_test", 50);
        mongoDBCollectionRemoveTest = createTestCollection("remove_test", 50);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mongoDataStoreManager.drop("datastore_test");
        mongoDataStore.close();
    }

    private static MongoDBCollection createTestCollection(String test, int size) {
        MongoDBCollection mongoDBCollection = mongoDataStore.getCollection(test);
        DBObject dbObject = new BasicDBObject();
        for(int i = 0; i < size; i++) {
            dbObject = new BasicDBObject("id", i);
            dbObject.put("name", "John");
            dbObject.put("surname", "Doe");
            mongoDBCollection.nativeQuery().insert(dbObject, null);
        }
        return mongoDBCollection;
    }

    @Ignore
    @Test
    public void testQueryResultWriter() throws Exception {

        for (int i = 0; i < 100; i++) {
            mongoDBCollection.insert(new BasicDBObject("id", i), null);
        }

        BasicQueryResultWriter queryResultWriter = new BasicQueryResultWriter();
        mongoDBCollection.setQueryResultWriter(queryResultWriter);
        QueryResult<DBObject> dbObjectQueryResult = mongoDBCollection.find(new BasicDBObject("id", new BasicDBObject("$gt", 50)), null);
        System.out.println(dbObjectQueryResult);
        assert (dbObjectQueryResult.getResult().isEmpty());

        mongoDBCollection.setQueryResultWriter(null);
        dbObjectQueryResult = mongoDBCollection.find(new BasicDBObject("id", new BasicDBObject("$gt", 50)), null);
        System.out.println(dbObjectQueryResult);
        assert (!dbObjectQueryResult.getResult().isEmpty());

    }

    @Test
    public void testDistinct() throws Exception {
        QueryResult<Object> id1 = mongoDBCollection.distinct("id", null);
        QueryResult<Integer> id2 = mongoDBCollection.distinct("id", null,  new ComplexTypeConverter<Integer, Object>() {
            @Override
            public Integer convertToDataModelType(Object object) {
                if(object instanceof Integer) {
                    return (Integer) object;
                } else {
                    System.out.println("Non integer result : " + object);
                    return 0;
                }
            }
            @Override
            public Object convertToStorageType(Integer object) { return null; }
        });
//        System.out.println(mongoDBCollection.distinct("name", null).getNumResults());
//        System.out.println(mongoDBCollection.nativeQuery().distinct("name"));
    }

    @Test
    public void testCount() throws Exception {
        QueryResult<Long> queryResult = mongoDBCollection.count();
        assertEquals("The number of documents must be equals", new Long(N), queryResult.getResult().get(0));
    }

    @Test
    public void testCount1() throws Exception {
        QueryResult<Long> queryResult = mongoDBCollection.count();
        assertEquals("The number must be equals", new Long(N), queryResult.first());
    }

    @Test
    public void testDistinct1() throws Exception {
        QueryResult<Object> queryResult = mongoDBCollection.distinct("id", null);
//        System.out.println("queryResult = " + queryResult);
    }

    @Test
    public void testDistinct2() throws Exception {
        QueryResult<HashMap> queryResult = mongoDBCollection.distinct("id", null, HashMap.class);
//        System.out.println("queryResult = " + queryResult);
    }

    @Test
    public void testDistinct3() throws Exception {

    }

    @Test
    public void testFind() throws Exception {
        DBObject dbObject = new BasicDBObject("id", 4);
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("id"));
        QueryResult<DBObject> queryResult = mongoDBCollection.find(dbObject, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertEquals("Returned Id does not match", 4, queryResult.first().get("id"));
//        System.out.println("queryResult 'include' = " + queryResult);
    }

    @Test
    public void testFind1() throws Exception {
        DBObject dbObject = new BasicDBObject("id", 4);
        DBObject returnFields = new BasicDBObject("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<DBObject> queryResult = mongoDBCollection.find(dbObject, returnFields, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertNull("Field 'name' must not exist", queryResult.first().get("name"));
//        System.out.println("queryResult 'projection' = " + queryResult);
    }

    @Test
    public void testFind2() throws Exception {
        DBObject dbObject = new BasicDBObject("id", 4);
        DBObject returnFields = new BasicDBObject("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<HashMap> queryResult = mongoDBCollection.find(dbObject, returnFields, HashMap.class, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertTrue("Returned field must instance of Hashmap", queryResult.first() instanceof HashMap);
    }

    @Test
    public void testFind3() throws Exception {
        final DBObject dbObject = new BasicDBObject("id", 4);
        DBObject returnFields = new BasicDBObject("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        QueryResult<HashMap> queryResult = mongoDBCollection.find(dbObject, returnFields,
                new ComplexTypeConverter<HashMap, DBObject>() {
            @Override
            public HashMap convertToDataModelType(DBObject object) {
                return new HashMap(dbObject.toMap());
            }

            @Override
            public DBObject convertToStorageType(HashMap object) {
                return null;
            }
        }, queryOptions);
        assertNotNull("Object cannot be null", queryResult.getResult());
        assertTrue("Returned field must instance of Hashmap", queryResult.first() instanceof HashMap);
    }

    @Test
    public void testFind4() throws Exception {
        List<DBObject> dbObjectList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            dbObjectList.add(new BasicDBObject("id", i));
        }
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("id"));
        List<QueryResult<DBObject>> queryResultList = mongoDBCollection.find(dbObjectList, queryOptions);
        assertEquals("List must contain 10 results", 10, queryResultList.size());
        assertNotNull("Object cannot be null", queryResultList.get(0).getResult());
        assertEquals("Returned Id does not match", 9, queryResultList.get(9).first().get("id"));
    }

    @Test
    public void testFind5() throws Exception {
        List<DBObject> dbObjectList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            dbObjectList.add(new BasicDBObject("id", i));
        }
        DBObject returnFields = new BasicDBObject("id", 1);
        QueryOptions queryOptions = new QueryOptions("exclude", Arrays.asList("id"));
        List<QueryResult<DBObject>> queryResultList = mongoDBCollection.find(dbObjectList, returnFields, queryOptions);
        assertEquals("List must contain 10 results", 10, queryResultList.size());
        assertNotNull("Object cannot be null", queryResultList.get(0).getResult());
        assertNull("Field 'name' must not exist", queryResultList.get(0).first().get("name"));
    }

    @Test
    public void testFind6() throws Exception {

    }

    @Test
    public void testFind7() throws Exception {

    }

    @Test
    public void testAggregate() throws Exception {

    }

    @Test
    public void testInsert() throws Exception {

    }

    @Test
    public void testInsert1() throws Exception {

    }

    @Test
    public void testUpdate() throws Exception {

    }

    @Test
    public void testUpdate1() throws Exception {

    }

    @Test
    public void testRemove() throws Exception {

    }

    @Test
    public void testFindAndModify() throws Exception {

    }

    @Test
    public void testFindAndModify1() throws Exception {

    }

    @Test
    public void testFindAndModify2() throws Exception {

    }

    @Test
    public void testCreateIndex() throws Exception {

    }

    @Test
    public void testDropIndex() throws Exception {

    }

    @Test
    public void testGetIndex() throws Exception {

    }

    class BasicQueryResultWriter implements QueryResultWriter<DBObject> {
        int i = 0;
        String outfile = "/tmp/queryResultWriter.log";
        DataOutputStream fileOutputStream;

        @Override
        public void open() throws IOException {
            System.out.println("Opening!");
            this.fileOutputStream = new DataOutputStream(new FileOutputStream(outfile));
        }

        @Override
        public void write(DBObject elem) throws IOException {
            String s = String.format("Result %d : %s\n", i++, elem.toString());
            System.out.printf(s);
            fileOutputStream.writeBytes(s);
        }

        @Override
        public void close() throws IOException {
            System.out.println("Closing!");
            fileOutputStream.close();
        }
    }

}

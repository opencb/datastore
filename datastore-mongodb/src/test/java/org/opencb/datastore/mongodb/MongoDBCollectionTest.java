package org.opencb.datastore.mongodb;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.QueryResultWriter;

/**
 * Created by imedina on 29/03/14.
 */
public class MongoDBCollectionTest {

    private static MongoDataStoreManager mongoDataStoreManager;
    private static MongoDataStore mongoDataStore;
//    private static MongoDBCollection mongoDBCollection;
    private static MongoDBCollection mongoDBCollectionTest;


    @BeforeClass
    public static void setUp() throws Exception {
        mongoDataStoreManager = new MongoDataStoreManager("localhost", 27017);
        mongoDataStore = mongoDataStoreManager.get("test");
        mongoDBCollectionTest = mongoDataStore.getCollection("test");

//        mongoDBCollection = mongoDataStore.getCollection("protein");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        mongoDataStore.close();
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

    @Test
    public void testQueryResultWriter() throws Exception {

        for (int i = 0; i < 100; i++) {
            mongoDBCollectionTest.insert(new BasicDBObject("id", i));
        }

        BasicQueryResultWriter queryResultWriter = new BasicQueryResultWriter();
        mongoDBCollectionTest.setQueryResultWriter(queryResultWriter);
        QueryResult<DBObject> dbObjectQueryResult = mongoDBCollectionTest.find(new BasicDBObject("id", new BasicDBObject("$gt", 50)), null);
        System.out.println(dbObjectQueryResult);
        assert (dbObjectQueryResult.getResult().isEmpty());

        mongoDBCollectionTest.setQueryResultWriter(null);
        dbObjectQueryResult = mongoDBCollectionTest.find(new BasicDBObject("id", new BasicDBObject("$gt", 50)), null);
        System.out.println(dbObjectQueryResult);
        assert (!dbObjectQueryResult.getResult().isEmpty());

    }

    @Test
    public void testCount() throws Exception {
//        System.out.println(mongoDBCollection.count().getResult());
//        System.out.println(mongoDBCollection.nativeQuery().count());
    }

    @Test
    public void testDistinct() throws Exception {
        QueryResult<Integer> id1 = mongoDBCollectionTest.distinct("id", null);
        QueryResult<Integer> id2 = mongoDBCollectionTest.distinct("id", new ComplexTypeConverter<Integer, Object>() {
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
    public void testFind() throws Exception {
//        DBObject dbObject = new BasicDBObject("name", "STAC_HUMAN");
//        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("accession"));
//        System.out.println(mongoDBCollection.find(dbObject, queryOptions).getResult());
//        System.out.println(mongoDBCollection.nativeQuery().find(dbObject, null));
    }

    @Test
    public void testAggregate() throws Exception {

    }
}

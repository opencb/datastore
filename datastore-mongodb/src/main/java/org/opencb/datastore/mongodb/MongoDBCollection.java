package org.opencb.datastore.mongodb;

import com.google.common.collect.Lists;
import java.util.*;
import com.mongodb.*;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 * Created by imedina on 28/03/14.
 */
public class MongoDBCollection {

    private DBCollection dbCollection;

    private MongoDBNativeQuery mongoDBNativeQuery;
    private long start;
    private long end;

    MongoDBCollection(DBCollection dbCollection) {
        this.dbCollection = dbCollection;
        mongoDBNativeQuery = new MongoDBNativeQuery(dbCollection);
    }


    private void startQuery() {
        start = System.currentTimeMillis();
    }
    
    private QueryResult endQuery(List result, Object resultType) {
        end = System.currentTimeMillis();
        int numResults = (result != null) ? result.size() : 0;
        return new QueryResult(null, (int) (end-start), numResults, numResults, null, null, result);
    }
    
    private QueryResult endQuery(List result, Object resultType, int numResults) {
        end = System.currentTimeMillis();
        return new QueryResult(null, (int) (end-start), (result != null) ? result.size() : 0, numResults, null, null, result);
    }
    
    public QueryResult count() {
        startQuery();
        long l = mongoDBNativeQuery.count();
        System.out.println(dbCollection.getStats());
        return endQuery(Arrays.asList(l), Long.class);
    }

    public QueryResult count(DBObject query) {
        startQuery();
        long l = mongoDBNativeQuery.count(query);
        return endQuery(Arrays.asList(l), Long.class);
    }


    public QueryResult distinct(String key) {
        startQuery();
        List l = mongoDBNativeQuery.distinct(key);
        return endQuery(l, List.class);
    }

    public QueryResult distinct(String key, DBObject query) {
        startQuery();
        List l = mongoDBNativeQuery.distinct(key, query);
        return endQuery(l, List.class);
    }


    public QueryResult find(DBObject query, QueryOptions options) {
        return find(query, null, options);
    }

    public QueryResult find(DBObject query, DBObject returnFields, QueryOptions options) {
        startQuery();
        QueryResult queryResult;
        DBCursor cursor = mongoDBNativeQuery.find(query, returnFields, options);
        BasicDBList list = new BasicDBList();
        
        try {
            if (cursor != null) {
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
            
                if (options != null && options.containsKey("limit")) {
                    queryResult = endQuery(list, BasicDBList.class, cursor.count());
                } else {
                    queryResult = endQuery(list, BasicDBList.class);
                }
                
            } else {
                queryResult = endQuery(list, BasicDBList.class);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        
        return queryResult;
    }

    public QueryResult aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        startQuery();
        QueryResult queryResult = new QueryResult();
        AggregationOutput output = mongoDBNativeQuery.aggregate(id, operations, options);
        queryResult.setResult(Lists.newArrayList(output.results()));
        queryResult.setNumTotalResults(queryResult.getNumResults());
        return queryResult;
    }

    public QueryResult insert(DBObject... object) {
        startQuery();
        WriteResult wr = mongoDBNativeQuery.insert(object);
        QueryResult queryResult = endQuery(Arrays.asList(wr), WriteResult.class);
        if (!wr.getLastError().ok()) {
            queryResult.setErrorMsg(wr.getLastError().getErrorMessage());
        }
        return queryResult;
    }

    public QueryResult update(DBObject object, DBObject updates, boolean upsert, boolean multi) {
        startQuery();
        WriteResult wr = mongoDBNativeQuery.update(object, updates, upsert, multi);
        QueryResult queryResult = endQuery(Arrays.asList(wr), WriteResult.class);
        if (!wr.getLastError().ok()) {
            queryResult.setErrorMsg(wr.getLastError().getErrorMessage());
        }
        return queryResult;
    }


    /**
     * Create a new Native instance.  This is a convenience method, equivalent to {@code new MongoClientOptions.Native()}.
     *
     * @return a new instance of a Native
     */
    public MongoDBNativeQuery nativeQuery() {
        return mongoDBNativeQuery;
    }

}

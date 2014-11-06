package org.opencb.datastore.mongodb;

import com.mongodb.*;

import java.io.IOException;
import java.util.*;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.QueryResultWriter;

/**
 * @author Ignacio Medina &lt;imedina@ebi.ac.uk&gt;
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public class MongoDBCollection {

    private DBCollection dbCollection;

    private MongoDBNativeQuery mongoDBNativeQuery;
    private long start;
    private long end;
    private QueryResultWriter<DBObject> queryResultWriter;

    MongoDBCollection(DBCollection dbCollection) {
        this.dbCollection = dbCollection;
        mongoDBNativeQuery = new MongoDBNativeQuery(dbCollection);
        queryResultWriter = null;
    }


    private void startQuery() {
        start = System.currentTimeMillis();
    }

    private <T> QueryResult<T> endQuery(List result, ComplexTypeConverter converter) {
        int numResults = (result != null) ? result.size() : 0;
        return endQuery(result, converter, numResults);
    }

    private <T> QueryResult<T> endQuery(List result, ComplexTypeConverter converter, int numTotalResults) {
        end = System.currentTimeMillis();
        int numResults = (result != null) ? result.size() : 0;

        QueryResult<T> queryResult = new QueryResult(null, (int) (end-start), numResults, numTotalResults, null, null, result);
        // If a converter is provided, convert DBObjects to the requested type
        if (converter != null) {
            List convertedResult = new ArrayList<>(numResults);
            for (Object o : result) {
                convertedResult.add(converter.convertToDataModelType(o));
            }
            queryResult.setResult(convertedResult);
        } else {
            queryResult.setResult(result);
        }

        return queryResult;

    }
    
    public QueryResult<Long> count() {
        startQuery();
        long l = mongoDBNativeQuery.count();
        System.out.println(dbCollection.getStats());
        return endQuery(Arrays.asList(l), null);
    }

    public QueryResult<Long> count(DBObject query) {
        startQuery();
        long l = mongoDBNativeQuery.count(query);
        return endQuery(Arrays.asList(l), null);
    }

    public <T, O> QueryResult<T> distinct(String key, ComplexTypeConverter<T, O> converter) {
        return distinct(key, null, converter);
    }

    public <T, O> QueryResult<T> distinct(String key, DBObject query, ComplexTypeConverter<T, O> converter) {
        startQuery();
        List<O> l = mongoDBNativeQuery.distinct(key, query);
        return endQuery(l, converter);
    }

    public QueryResult<DBObject> find(DBObject query, QueryOptions options) {
        return find(query, options, null, null);
    }

    public QueryResult<DBObject> find(DBObject query, QueryOptions options, DBObject returnFields) {
        return find(query, options, null, returnFields);
    }

    public <T> QueryResult<T> find(DBObject query, QueryOptions options, ComplexTypeConverter<T, DBObject> converter) {
        return find(query, options, converter, null);
    }

    public <T> QueryResult<T> find(DBObject query, QueryOptions options, ComplexTypeConverter<T, DBObject> converter, DBObject returnFields) {
        startQuery();
        QueryResult<T> queryResult;
        DBCursor cursor = mongoDBNativeQuery.find(query, returnFields, options);
        //BasicDBList list = new BasicDBList();
        List<DBObject> list = new LinkedList<>();

        if (cursor != null) {
            if (queryResultWriter != null) {
                try {
                    queryResultWriter.open();
                    while (cursor.hasNext()) {
                        queryResultWriter.write(cursor.next());
                    }
                    queryResultWriter.close();
                } catch (IOException e) {
                    cursor.close();
                    queryResult = endQuery(null, converter);
                    queryResult.setErrorMsg(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
                    return queryResult;
                }
            } else {
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
            }
            
            if (options != null && options.getInt("limit") > 0) {
                queryResult = endQuery(list, converter, cursor.count());
            } else {
                queryResult = endQuery(list, converter);
            }
            
            cursor.close();
        } else {
            queryResult = endQuery(list, converter);
        }
        
        return queryResult;
    }

    public QueryResult<DBObject> aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        startQuery();
        QueryResult<DBObject> queryResult;
        AggregationOutput output = mongoDBNativeQuery.aggregate(id, operations, options);
        Iterator<DBObject> iterator = output.results().iterator();
        List<DBObject> list = new LinkedList<>();
        if (queryResultWriter != null) {
            try {
                queryResultWriter.open();
                while (iterator.hasNext()) {
                    queryResultWriter.write(iterator.next());
                }
                queryResultWriter.close();
            } catch (IOException e) {
                queryResult = endQuery(list, null);
                queryResult.setErrorMsg(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
                return queryResult;
            }
        } else {
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }

        queryResult = endQuery(list, null);
        queryResult.setResult(list);
        queryResult.setNumTotalResults(queryResult.getNumResults());
        return queryResult;
    }

    public QueryResult<WriteResult> insert(DBObject... object) {
        startQuery();
        WriteResult wr = mongoDBNativeQuery.insert(object);
        QueryResult<WriteResult> queryResult = endQuery(Arrays.asList(wr), null);
        if (!wr.getLastError().ok()) {
            queryResult.setErrorMsg(wr.getLastError().getErrorMessage());
        }
        return queryResult;
    }

    public QueryResult<WriteResult> update(DBObject object, DBObject updates, boolean upsert, boolean multi) {
        startQuery();
        WriteResult wr = mongoDBNativeQuery.update(object, updates, upsert, multi);
        QueryResult<WriteResult> queryResult = endQuery(Arrays.asList(wr), null);
        if (!wr.getLastError().ok()) {
            queryResult.setErrorMsg(wr.getLastError().getErrorMessage());
        }
        return queryResult;
    }

    public QueryResultWriter<DBObject> getQueryResultWriter() {
        return queryResultWriter;
    }

    public void setQueryResultWriter(QueryResultWriter<DBObject> queryResultWriter) {
        this.queryResultWriter = queryResultWriter;
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

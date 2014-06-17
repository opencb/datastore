package org.opencb.datastore.mongodb;

import com.google.common.collect.Lists;
import com.mongodb.*;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.datastore.core.ComplexTypeConverter;
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


    private QueryResult createQueryResult() {
        QueryResult queryResult = new QueryResult();
        start = System.currentTimeMillis();

        return queryResult;
    }

    private QueryResult prepareQueryResult(QueryResult queryResult, List result, Class resultType, ComplexTypeConverter converter) {
        return prepareQueryResult(queryResult, result, resultType, converter, queryResult.getNumResults());
    }

    private QueryResult prepareQueryResult(QueryResult queryResult, List result, Class resultType, ComplexTypeConverter converter, int numTotalResults) {
        end = System.currentTimeMillis();

        // If a converter is provided, convert DBObjects to the requested type
        if (converter != null) {
            List convertedResult = new ArrayList<>(result.size());
            for (Object o : result) {
                convertedResult.add(converter.convertToDataModelType(o));
            }
            queryResult.setResult(convertedResult);
        } else {
            queryResult.setResult(result);
        }
        
        queryResult.setNumResults((result != null) ? result.size() : 0);
        queryResult.setNumTotalResults(numTotalResults);
        queryResult.setResultType(resultType);
        queryResult.setDBTime((int)(end-start));

        return queryResult;
    }


    public QueryResult count() {
        QueryResult queryResult = createQueryResult();
        long l = mongoDBNativeQuery.count();
        return prepareQueryResult(queryResult, Arrays.asList(l), Long.class, null);
    }

    public QueryResult count(DBObject query) {
        QueryResult queryResult = createQueryResult();
        long l = mongoDBNativeQuery.count(query);
        return prepareQueryResult(queryResult, Arrays.asList(l), Long.class, null);
    }


    public QueryResult distinct(String key, ComplexTypeConverter converter) {
        return distinct(key, null, converter);
    }

    public QueryResult distinct(String key, DBObject query, ComplexTypeConverter converter) {
        QueryResult queryResult = createQueryResult();
        List l = mongoDBNativeQuery.distinct(key, query);
        try {
            return prepareQueryResult(queryResult, l,
                    converter == null ? DBObject.class :
                            Class.forName((((ParameterizedType) converter.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0])
                                    .getClass().getCanonicalName()),
                    converter);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MongoDBCollection.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return queryResult;
    }


    public QueryResult find(DBObject query, QueryOptions options, ComplexTypeConverter converter) {
        return find(query, options, converter, null);
    }

    public QueryResult find(DBObject query, QueryOptions options, ComplexTypeConverter converter, DBObject returnFields) {
        QueryResult queryResult = createQueryResult();
        DBCursor cursor = mongoDBNativeQuery.find(query, returnFields, options);
        BasicDBList list = new BasicDBList();
        
        try {
            if (cursor != null) {
                while (cursor.hasNext()) {
                    list.add(cursor.next());
                }
            
                if (options != null && options.getInt("limit") > 0) {
                    queryResult = prepareQueryResult(queryResult, list, 
                            converter == null ? DBObject.class : 
                                    Class.forName((((ParameterizedType) converter.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0])
                                            .getClass().getCanonicalName()), 
                            converter, cursor.count());
                } else {
                    queryResult = prepareQueryResult(queryResult, list, 
                            converter == null ? DBObject.class : 
                                    Class.forName((((ParameterizedType) converter.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0])
                                            .getClass().getCanonicalName()), 
                            converter);
                }
                
            } else {
                queryResult = prepareQueryResult(queryResult, list, 
                            converter == null ? DBObject.class : 
                                    Class.forName((((ParameterizedType) converter.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0])
                                            .getClass().getCanonicalName()), 
                            converter);
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MongoDBCollection.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        
        return queryResult;
    }

    public QueryResult aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        QueryResult queryResult = createQueryResult();
        AggregationOutput output = mongoDBNativeQuery.aggregate(id, operations, options);
        queryResult.setResult(Lists.newArrayList(output.results()));
        return queryResult;
    }

    public QueryResult insert(DBObject... object) {
        QueryResult queryResult = createQueryResult();
        WriteResult wr = mongoDBNativeQuery.insert(object);
        prepareQueryResult(queryResult, Arrays.asList(wr), WriteResult.class, null);
        if (!wr.getLastError().ok()) {
            queryResult.setError(wr.getLastError());
        }
        return queryResult;
    }

    public QueryResult update(DBObject object, DBObject updates, boolean upsert, boolean multi) {
        QueryResult queryResult = createQueryResult();
        WriteResult wr = mongoDBNativeQuery.update(object, updates, upsert, multi);
        prepareQueryResult(queryResult, Arrays.asList(wr), WriteResult.class, null);
        if (!wr.getLastError().ok()) {
            queryResult.setError(wr.getLastError());
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

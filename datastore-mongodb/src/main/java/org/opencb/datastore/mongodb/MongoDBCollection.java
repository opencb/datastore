package org.opencb.datastore.mongodb;

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


    private QueryResult createQueryResult() {
        QueryResult queryResult = new QueryResult();
        start = System.currentTimeMillis();

        return queryResult;
    }

    private QueryResult prepareQueryResult(List result, Object resultType, QueryResult queryResult) {
        end = System.currentTimeMillis();

        queryResult.setResult(result);
        queryResult.setResultType(resultType);
        queryResult.setDBTime((int)(end-start));

        return queryResult;
    }


    public QueryResult count() {
        QueryResult queryResult = createQueryResult();
        long l = mongoDBNativeQuery.count();
        System.out.println(dbCollection.getStats());
        return prepareQueryResult(Arrays.asList(l), Long.class, queryResult);
    }

    public QueryResult distinct(String key, DBObject query) {
        QueryResult queryResult = createQueryResult();
        List l = mongoDBNativeQuery.distinct(key, query);
        queryResult.setNumResults(l.size());
        return prepareQueryResult(l, List.class, queryResult);
    }

    public QueryResult find(DBObject query, QueryOptions options) {
        QueryResult queryResult = createQueryResult();
        BasicDBList l = mongoDBNativeQuery.find(query, options);
        return prepareQueryResult(l, BasicDBList.class, queryResult);
    }

    public QueryResult find(DBObject query, DBObject returnFields, QueryOptions options) {
        QueryResult queryResult = createQueryResult();
        BasicDBList l = mongoDBNativeQuery.find(query, returnFields, options);
        return prepareQueryResult(l, BasicDBList.class, queryResult);
    }


    protected QueryResult aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        QueryResult queryResult = createQueryResult();

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

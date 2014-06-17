package org.opencb.datastore.mongodb;

import com.mongodb.*;
import java.util.List;
import org.opencb.datastore.core.QueryOptions;


/**
 * Created by imedina on 28/03/14.
 */
class MongoDBNativeQuery {

    private final DBCollection dbCollection;

    MongoDBNativeQuery(DBCollection dbCollection) {
        this.dbCollection = dbCollection;
    }

    public long count() {
        long result = dbCollection.count();
        return result;
    }

    public long count(DBObject query) {
        long result = dbCollection.count(query);
        return result;
    }
    
    public List distinct(String key) {
        return distinct(key, null);
    }

    public List distinct(String key, DBObject query) {
        List result = dbCollection.distinct(key, query);
        return result;
    }

    public DBCursor find(DBObject query, QueryOptions options) {
        return find(query, null, options);
    }

    public DBCursor find(DBObject query, DBObject returnFields, QueryOptions options) {
        DBCursor cursor;

        if(returnFields == null) {
            returnFields = getReturnFields(options);
        }
        cursor = dbCollection.find(query, returnFields);

        int limit = (options != null) ? options.getInt("limit", 0) : 0;
        if (limit > 0) {
            cursor.limit(limit);
        }
        
        int skip = (options != null) ? options.getInt("skip", 0) : 0;
        if (skip > 0) {
            cursor.skip(skip);
        }

        BasicDBObject sort = (options != null) ? (BasicDBObject) options.get("sort") : null;
        if (sort != null) {
            cursor.sort(sort);
        }
 
        return cursor;
    }

    public AggregationOutput aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        return (operations.size() > 0) ? dbCollection.aggregate(operations) : null;
    }

    public WriteResult insert(DBObject... objects) {
        return dbCollection.insert(objects);
    }

    public WriteResult update(DBObject object, DBObject updates, boolean upsert, boolean multi) {
        return dbCollection.update(object, updates, upsert, multi);
    }

    private BasicDBObject getReturnFields(QueryOptions options) {
        // Select which fields are excluded and included in the query
        BasicDBObject returnFields = new BasicDBObject("_id", 0);
        if (options != null) {
            // Read and process 'include'/'exclude' field from 'options' object
            if (options.getList("include") != null && options.getList("include").size() > 0) {
                for (Object field : options.getList("include")) {
                    returnFields.put(field.toString(), 1);
                }
            } else {
                if (options.getList("exclude") != null && options.getList("exclude").size() > 0) {
                    for (Object field : options.getList("exclude")) {
                        returnFields.put(field.toString(), 0);
                    }
                }
            }
        }
        return returnFields;
    }

}

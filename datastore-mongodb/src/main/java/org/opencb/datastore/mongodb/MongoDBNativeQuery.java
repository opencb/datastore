package org.opencb.datastore.mongodb;

import com.mongodb.*;
import java.util.List;
import org.opencb.datastore.core.QueryOptions;


/**
 * Created by imedina on 28/03/14.
 */
public class MongoDBNativeQuery {

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

    public DBCursor find(DBObject query, DBObject projection, QueryOptions options) {
        DBCursor cursor;

        if(projection == null) {
            projection = getProjection(projection, options);
        }
        cursor = dbCollection.find(query, projection);

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

    public AggregationOutput aggregate(List<DBObject> operations, QueryOptions options) {
        return (operations.size() > 0) ? dbCollection.aggregate(operations) : null;
    }

    public WriteResult insert(DBObject objects, QueryOptions options) {
        return dbCollection.insert(objects);
    }

    public WriteResult insert(List<DBObject> dbObjects, QueryOptions options) {
        return dbCollection.insert(dbObjects);
    }

    public WriteResult update(DBObject object, DBObject updates, boolean upsert, boolean multi) {
        return dbCollection.update(object, updates, upsert, multi);
    }

    // TODO This method must be implemented with Bulk
    public WriteResult update(List<DBObject> dbObjects, List<DBObject> updates, boolean upsert, boolean multi) {
        return null;
    }

    public WriteResult remove(DBObject query) {
        return dbCollection.remove(query);
    }

    public DBObject findAndModify(DBObject query, DBObject projection, DBObject sort, DBObject update, QueryOptions options) {
        boolean remove = false;
        boolean returnNew = false;
        boolean upsert = false;

        if(options != null) {
            if(projection == null) {
                projection = getProjection(projection, options);
            }
            remove = options.getBoolean("remove", false);
            returnNew = options.getBoolean("returnNew", false);
            upsert = options.getBoolean("upsert", false);
        }
        return dbCollection.findAndModify(query, projection, sort, remove, update, returnNew, upsert);
    }

    public void createIndex(DBObject keys, DBObject options) {
        dbCollection.createIndex(keys, options);
    }

    public List<DBObject> getIndex() {
        return dbCollection.getIndexInfo();
    }

    public void dropIndex(DBObject keys) {
        dbCollection.dropIndex(keys);
    }

    private DBObject getProjection(DBObject projection, QueryOptions options) {
        // Select which fields are excluded and included in the query
//      DBObject returnFields = null;
//      returnFields = new BasicDBObject("_id", 0);
        if(projection == null) {
            projection = new BasicDBObject();
        }
        projection.put("_id", 0);

        if (options != null) {
            // Read and process 'include'/'exclude' field from 'options' object
            List<String> includeStringList = options.getAsStringList("include", ",");
            if (includeStringList != null && includeStringList.size() > 0) {
                for (Object field : includeStringList) {
                    projection.put(field.toString(), 1);
                }
            } else {
                List<String> excludeStringList = options.getAsStringList("include", ",");
                if (excludeStringList != null && excludeStringList.size() > 0) {
                    for (Object field : excludeStringList) {
                        projection.put(field.toString(), 0);
                    }
                }
            }
        }
        return projection;
    }

}

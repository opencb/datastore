package org.opencb.datastore.mongodb;

import com.mongodb.*;
import java.util.Arrays;
import org.opencb.datastore.core.QueryOptions;

import java.util.Iterator;
import java.util.List;

/**
 * Created by imedina on 28/03/14.
 */
class MongoDBNativeQuery {

    private DBCollection dbCollection;

    MongoDBNativeQuery(DBCollection dbCollection) {
        this.dbCollection = dbCollection;
    }

    public long count() {
        long result = dbCollection.count();
        return result;
    }

    public List distinct(String key) {
        return distinct(key, null);
    }

    public List distinct(String key, DBObject query) {
        List result = dbCollection.distinct(key, query);
        return result;
    }

    public BasicDBList find(DBObject query, QueryOptions options) {
        return find(query, null, options);
    }

    public BasicDBList find(DBObject query, DBObject returnFields, QueryOptions options) {
        BasicDBList list = new BasicDBList();


        if (options != null && options.getBoolean("count")) {
            Long count = dbCollection.count(query);
            list.add(new BasicDBObject("count", count));
        }else {
            DBCursor cursor;
            if(returnFields != null) {
                cursor = dbCollection.find(query, returnFields);
            }else {
                returnFields = getReturnFields(options);
                cursor = dbCollection.find(query, returnFields);
            }

//            int limit = options.getInt("limit", 0);
            int limit = (options != null) ? options.getInt("limit", 0) : 0;
            if (limit > 0) {
                cursor.limit(limit);
            }
//            int skip = options.getInt("skip", 0);
            int skip = (options != null) ? options.getInt("skip", 0) : 0;
            if (skip > 0) {
                cursor.skip(skip);
            }

//            BasicDBObject sort = (BasicDBObject) options.get("sort");
            BasicDBObject sort = (options != null) ? (BasicDBObject) options.get("sort") : null;
            if (sort != null) {
                cursor.sort(sort);
            }

            try {
                if (cursor != null) {
                    while (cursor.hasNext()) {
                        list.add(cursor.next());
                    }
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }
        return list;
    }


    public AggregationOutput aggregate(Object id, List<DBObject> operations, QueryOptions options) {
        AggregationOutput aggregationOutput = null;

        // MongoDB aggregate method signature is: public AggregationOutput aggregate( DBObject firstOp, DBObject ... additionalOps)
        // so the operations array must be decomposed,
        if(operations.size() > 0) {
            DBObject[] objects = (DBObject[])operations.toArray();
            DBObject firstOperation = objects[0];
            DBObject[] restObjects = Arrays.copyOfRange(objects, 1, objects.length);
            
            // TODO Check 'options' param for 'ReadPreference'
//            aggregationOutput = dbCollection.aggregate(operations);
            aggregationOutput = dbCollection.aggregate(firstOperation, restObjects);

            // TODO Will this be ever used?
//            BasicDBList list = new BasicDBList();
//            try {
//                if (aggregationOutput != null) {
//                    Iterator<DBObject> results = aggregationOutput.results().iterator();
//                    while (results.hasNext()) {
//                        list.add(results.next());
//                    }
//                }
//            } finally {
//
//            }
        }

        return aggregationOutput;
    }


    private BasicDBObject getReturnFields(QueryOptions options) {
        // Select which fields are excluded and included in MongoDB query
        BasicDBObject returnFields = new BasicDBObject("_id", 0);
        if(options != null) {
            // Read and process 'exclude' field from 'options' object
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

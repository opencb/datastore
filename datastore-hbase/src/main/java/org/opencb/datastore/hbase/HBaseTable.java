package org.opencb.datastore.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HBaseTable {
    
    private HBaseAdmin admin;
    private NamespaceDescriptor namespace;
    private HTable table;
    
    private HBaseNativeQuery hbaseNativeQuery;

    private long start;
    private long end;
    
    HBaseTable(HBaseAdmin admin, NamespaceDescriptor namespace, String tableName, String[] columnFamilies) throws IOException {
        this.admin = admin;
        this.namespace = namespace;
        
        if (!admin.tableExists(tableName)) {
            table = createTable(admin, namespace, tableName, columnFamilies);
        }
        
        this.hbaseNativeQuery = new HBaseNativeQuery(admin, table);
    }
    
    private HTable createTable(HBaseAdmin admin, NamespaceDescriptor db, String tableName, String[] columnFamilies) throws IOException {
        HTable newTable = new HTable(admin.getConfiguration(), db.getName() + ":" + tableName);
        HTableDescriptor descriptor = newTable.getTableDescriptor();
        
        for (String family : columnFamilies) {
            HColumnDescriptor columnFamily = new HColumnDescriptor(family);
            columnFamily.setCompressionType(Compression.Algorithm.SNAPPY);
            descriptor.addFamily(columnFamily);
        }
        
        // Create table
        admin.createTable(descriptor);
        return newTable;
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
        try {
            long l = hbaseNativeQuery.count();
            queryResult = prepareQueryResult(Arrays.asList(l), Long.class, queryResult);
        } catch (Throwable ex) {
            queryResult = prepareQueryResult(null, Long.class, queryResult);
            queryResult.setError(ex.getMessage());
        }
        return queryResult;
    }

    // TODO How to do this with HBase?
//    public QueryResult distinct(String key, DBObject query) {
//        QueryResult queryResult = createQueryResult();
//        List l = mongoDBNativeQuery.distinct(key, query);
//        queryResult.setNumResults(l.size());
//        return prepareQueryResult(l, List.class, queryResult);
//    }

    public QueryResult find(String rowkey, QueryOptions options) throws IOException {
        QueryResult queryResult = createQueryResult();
        Result r = hbaseNativeQuery.find(rowkey, null, options);
//        return prepareQueryResult(l, BasicDBList.class, queryResult);
        return queryResult;
    }

    public QueryResult find(String rowkey, List<String> returnFields, QueryOptions options) throws IOException {
        QueryResult queryResult = createQueryResult();
        Result r = hbaseNativeQuery.find(rowkey, returnFields, options);
//        return prepareQueryResult(l, BasicDBList.class, queryResult);
        return queryResult;
    }

    public QueryResult find(String startRow, String endRow, QueryOptions options) throws IOException {
        QueryResult queryResult = createQueryResult();
        Iterator<Result> r = hbaseNativeQuery.find(startRow, endRow, null, options);
//        return prepareQueryResult(l, BasicDBList.class, queryResult);
        return queryResult;
    }

    public QueryResult find(String startRow, String endRow, List<String> returnFields, QueryOptions options) throws IOException {
        QueryResult queryResult = createQueryResult();
        Iterator<Result> r = hbaseNativeQuery.find(startRow, endRow, returnFields, options);
//        return prepareQueryResult(l, BasicDBList.class, queryResult);
        return queryResult;
    }
    
    /**
     * Create a new Native instance.  This is a convenience method.
     *
     * @return a new instance of a Native
     */
    public HBaseNativeQuery nativeQuery() {
        return hbaseNativeQuery;
    }


}

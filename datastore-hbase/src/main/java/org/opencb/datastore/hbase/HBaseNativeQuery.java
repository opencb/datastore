package org.opencb.datastore.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.opencb.datastore.core.QueryOptions;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public class HBaseNativeQuery {
    
    private final HBaseAdmin admin;
    private final HTable table;
    
    HBaseNativeQuery(HBaseAdmin admin, HTable table) {
        this.admin = admin;
        this.table = table;
    }

    public long count() throws Throwable {
        Configuration configuration = admin.getConfiguration();
        AggregationClient aggregationClient = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addFamily((table.getTableDescriptor().getColumnFamilies()[0]).toByteArray());
        return aggregationClient.rowCount(table.getName(), null, scan);
    }

    public List distinct(String field) {
        throw new UnsupportedOperationException("Querying distinct values is not implemented yet");
    }
    
    /**
     * Returns the result from a query to a single row, performed using a Get 
     * object from HBase API.
     * 
     * @param rowKey Row key to query
     * @param returnFields List of fields to return, in pairs of format cf:col
     * @param options
     * @return
     * @throws IOException 
     */
    public Result find(String rowKey, List<String> returnFields, QueryOptions options) throws IOException {
        Get get = new Get(rowKey.getBytes());
        
        if (returnFields != null) {
            for (String field : returnFields) {
                String[] parts = field.split(":");
                get.addColumn(parts[0].getBytes(), parts[1].getBytes());
            }
        } else {
            getReturnFields(get, options);
        }

        int maxVersions = (options != null) ? options.getInt("maxVersions", 0) : 0;
        if (maxVersions > 0) {
            get.setMaxVersions(maxVersions);
        }
        
        return table.get(get);
    }
    
    /**
     * Returns the results from a query to multiple rows, performed using a Scan 
     * object from HBase API.
     * 
     * @param startRow First row key to query
     * @param endRow Last row key to query
     * @param returnFields List of fields to return, in pairs of format cf:col
     * @param options
     * @return
     * @throws IOException 
     * @see <a href="https://stackoverflow.com/questions/17981450/row-pagination-with-hbase">Instructions on how to perform pagination in HBase</a>
     */
    public Iterator<Result> find(String startRow, String endRow, List<String> returnFields, QueryOptions options) throws IOException {
        Scan scan = new Scan(startRow.getBytes(), endRow.getBytes());
        
        if (returnFields != null) {
            for (String field : returnFields) {
                String[] parts = field.split(":");
                scan.addColumn(parts[0].getBytes(), parts[1].getBytes());
            }
        } else {
            getReturnFields(scan, options);
        }

        int maxVersions = (options != null) ? options.getInt("maxVersions", 0) : 0;
        if (maxVersions > 0) {
            scan.setMaxVersions(maxVersions);
        }
        
        int limit = (options != null) ? options.getInt("limit", 0) : 0;
        if (limit > 0) {
            scan.setFilter(new PageFilter(limit));
        }
        
        String sort = (options != null) ? options.getString("sort") : null;
        if (sort != null) {
            if (sort.equalsIgnoreCase("asc")) {
                scan.setReversed(false);
            } else if (sort.equalsIgnoreCase("desc")) {
                scan.setReversed(true);
            }
        }
        
        ResultScanner scanres = table.getScanner(scan);
        return scanres.iterator();
    }

    
    private Get getReturnFields(Get get, QueryOptions options) {
        // Select which fields are excluded and included in the query
        if(options != null) {
            // Read and process 'include'/'exclude' field from 'options' object
            if (options.getList("include") != null) {
                for (String field : options.getAsList("include", String.class)) {
                    String[] parts = field.split(":");
                    get.addColumn(parts[0].getBytes(), parts[1].getBytes());
                }
            } else if (options.getList("exclude") != null) {
                for (String field : options.getAsList("exclude", String.class)) {
                    // TODO Can it be done?
                }
            }
        }
        return get;
    }

    private Scan getReturnFields(Scan scan, QueryOptions options) {
        // Select which fields are excluded and included in the query
        if(options != null) {
            // Read and process 'include'/'exclude' field from 'options' object
            if (options.getList("include") != null) {
                for (String field : options.getAsList("include", String.class)) {
                    String[] parts = field.split(":");
                    scan.addColumn(parts[0].getBytes(), parts[1].getBytes());
                }
            } else if (options.getList("exclude") != null) {
                for (String field : options.getAsList("exclude", String.class)) {
                    // TODO Can it be done?
                }
            }
        }
        return scan;
    }

}

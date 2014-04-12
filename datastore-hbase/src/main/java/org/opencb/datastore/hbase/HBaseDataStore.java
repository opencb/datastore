package org.opencb.datastore.hbase;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HBaseDataStore {

    private static Map<String, HBaseTable> hbaseTables;

    private HBaseAdmin hbaseClient;
    private NamespaceDescriptor namespace;
    private HBaseConfiguration hbaseConfiguration;

    protected Logger logger;

    HBaseDataStore(HBaseAdmin hbaseClient, NamespaceDescriptor namespace, HBaseConfiguration hbaseConfiguration) {
        this.hbaseClient = hbaseClient;
        this.namespace = namespace;
        this.hbaseConfiguration = hbaseConfiguration;
        init();
    }

    @Deprecated
    HBaseDataStore(String namespace, HBaseConfiguration mongoDBConfiguration) throws IOException {
        hbaseClient.createNamespace(NamespaceDescriptor.create(namespace).build());
        this.namespace = hbaseClient.getNamespaceDescriptor(namespace);
        this.hbaseConfiguration = mongoDBConfiguration;
    }

    private void init() {
        hbaseTables = new HashMap<>();
        logger = LoggerFactory.getLogger(HBaseDataStore.class);
    }

    public HBaseTable createTable(String tableName, String... columnFamilies) throws IOException {
        HBaseTable table = new HBaseTable(hbaseClient, namespace, tableName, columnFamilies);
        hbaseTables.put(tableName, table);
        return table;
    }

    public HBaseTable getTable(String tableName) {
        return hbaseTables.get(tableName);
    }

    public boolean test() {
        try {
            HBaseAdmin.checkHBaseAvailable(hbaseClient.getConfiguration());
        } catch (ZooKeeperConnectionException ex) {
            return false;
        } catch (ServiceException | IOException ex) {
            return false;
        }

        return true;
    }

    public void close() throws IOException {
        logger.info("MongoDataStore: connection closed");
        hbaseClient.close();
    }

    public static Map<String, HBaseTable> getHBaseTables() {
        return hbaseTables;
    }

    public NamespaceDescriptor getNamespace() {
        return namespace;
    }

    public String getDatabaseName() {
        return namespace.getName();
    }

    public HBaseConfiguration getHBaseConfiguration() {
        return hbaseConfiguration;
    }

}

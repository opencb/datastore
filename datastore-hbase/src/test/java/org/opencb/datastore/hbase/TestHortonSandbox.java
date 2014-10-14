package org.opencb.datastore.hbase;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 */
public class TestHortonSandbox {

    @Test
    public void testConnection() throws IOException, MasterNotRunningException, ZooKeeperConnectionException, ServiceException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master.host", "sandbox.hortonworks.com");
        config.set("hbase.master.port", "8020");
        config.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        HBaseAdmin.checkHBaseAvailable(config);

        System.out.println("* * * Things look healthy * * *");
    }
}

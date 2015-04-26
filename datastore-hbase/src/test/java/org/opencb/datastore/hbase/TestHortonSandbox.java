/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

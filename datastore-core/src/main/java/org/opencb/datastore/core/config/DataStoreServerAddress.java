package org.opencb.datastore.core.config;

/**
 * Created by imedina on 23/03/14.
 */
public class DataStoreServerAddress {

    private String host;
    private int port;

    public DataStoreServerAddress() {
    }

    public DataStoreServerAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return host + "\t" + port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

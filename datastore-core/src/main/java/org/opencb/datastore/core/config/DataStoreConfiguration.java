package org.opencb.datastore.core.config;

import org.opencb.datastore.core.ObjectMap;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by imedina on 23/03/14.
 */
@Deprecated
public class DataStoreConfiguration extends ObjectMap {

    public DataStoreConfiguration() {

    }

    public DataStoreConfiguration(int size) {
        super(size);
    }

    public DataStoreConfiguration(final String key, final Object value) {
        super(key, value);
    }

    public DataStoreConfiguration(final Map<String, Object> inputOptions) {
        super(inputOptions);
    }

    public DataStoreConfiguration(String json) {
        super(json);
    }


    public void addConfiguration(final DataStoreConfiguration dataStoreConfiguration) {
        Iterator<String> iter = dataStoreConfiguration.keySet().iterator();
        while (iter.hasNext()) {
            String next =  iter.next();
            this.put(next, dataStoreConfiguration.get(next));
        }
    }

    public void addConfiguration(final Map<String, Object> inputOptions) {
        Iterator<String> iter = inputOptions.keySet().iterator();
        while (iter.hasNext()) {
            String next =  iter.next();
            this.put(next, inputOptions.get(next));
        }
    }

    public void setConfiguration(final Map<String, Object> inputOptions) {
        this.clear();
        this.putAll(inputOptions);
    }

    @Override
    public String toString() {
        return this.toString();
    }
}

package org.opencb.datastore.hbase;

import java.util.LinkedHashMap;
import java.util.Map;
import org.opencb.datastore.core.ObjectMap;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class HBaseConfiguration extends ObjectMap {
    
    HBaseConfiguration(final Map<String, Object> inputOptions) {
        super(inputOptions);
        this.putAll(inputOptions);
    }

    /**
     * A builder for HBaseConfiguration so that HBaseConfiguration can be immutable, 
     * and to support easier construction through chaining.
     */
    public static class Builder {
        private Map<String, Object> optionsMap;

        public Builder() {
            optionsMap = new LinkedHashMap<>();
        }

        public Builder init() {
            // There options will probably be not valid in production environments,
            // but it is a way to initialize development ones
            optionsMap.put("hbase.master.host", "localhost");
            optionsMap.put("hbase.master.port", "8020");
            optionsMap.put("hbase.zookeeper.quorum", "localhost");
            optionsMap.put("hbase.zookeeper.property.clientPort", "2181");
            return this;
        }

        public Builder load(final Map<String, Object> inputOptions) {
            optionsMap.putAll(inputOptions);
            return this;
        }

        public Builder add(String key, Object value) {
            optionsMap.put(key, value);
            return this;
        }

        public HBaseConfiguration build() {
            return new HBaseConfiguration(optionsMap);
        }

    }

    /**
     * Create a new Native instance.  This is a convenience method.
     *
     * @return a new instance of a Native
     */
    public static Builder builder() {
        return new Builder();
    }

}

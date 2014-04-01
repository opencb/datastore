package org.opencb.datastore.core;

import java.util.Map;

/**
 * Created by imedina on 20/03/14.
 */
public class QueryResponse extends ObjectMap {

    public QueryResponse() {
        initialize();
    }

    public QueryResponse(int size) {
        super(size);
        initialize();
    }

    public QueryResponse(final String key, final Object value) {
        initialize();
        // We must first initialize and then put the parameters
        this.put(key, value);
    }

    public QueryResponse(final Map<String, Object> map) {
        initialize();
        // We must first initialize and then put the parameters
        this.putAll(map);
    }

    public QueryResponse(String apiVersion, int time, Object warning, Object error, Object response) {
        this.put("apiVersion", apiVersion);
        this.put("time", time);
        this.put("warning", warning);
        this.put("error", error);
        this.put("response", response);
    }

    private void initialize() {
        this.put("apiVersion", "");
        this.put("time", "");
        this.put("warning", "");
        this.put("error", "");
        this.put("response", "");
    }


    public String getApiVersion() {
        return this.getString("apiVersion", "");
    }

    public void setApiVersion(String apiVersion) {
        this.put("apiVersion", apiVersion);
    }


    public int getTime() {
        return this.getInt("time", -1);
    }

    public void setDbVersion(int time) {
        this.put("time", time);
    }


    public Object getWarning() {
        return this.get("warning");
    }

    public void setWarning(Object value) {
        this.put("warning", value);
    }


    public Object getError() {
        return this.get("error");
    }

    public void setError(Object value) {
        this.put("error", value);
    }


    public Object getResponse() {
        return this.get("response");
    }

    public void setResponse(Object value) {
        this.put("response", value);
    }

}

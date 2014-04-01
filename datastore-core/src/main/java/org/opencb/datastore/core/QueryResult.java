package org.opencb.datastore.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 20/03/14.
 */
public class QueryResult extends ObjectMap {


    public QueryResult() {
        initialize();
    }

    public QueryResult(int size) {
        super(size);
        initialize();
    }

    public QueryResult(final String key, final Object value) {
        initialize();
        // We must first initialize and then put the parameters
        this.put(key, value);
    }

    public QueryResult(final Map<String, Object> map) {
        initialize();
        // We must first initialize and then put the parameters
        this.putAll(map);
    }

    public QueryResult(String json) {
        initialize();
        // We must first initialize and then put the parameters
        try {
            this.putAll(jsonObjectMapper.readValue(json, this.getClass()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public QueryResult(String id, int dbTime, int numResults, Object warning, Object error, Object resultType, List result) {
        this.put("id", id);
        this.put("dbTime", dbTime);
        this.put("numResults", numResults);
        this.put("warning", warning);
        this.put("error", error);
        this.put("resultType", resultType);
        this.put("result", result);
    }

    private void initialize() {
        this.put("id", "");
        this.put("dbTime", -1);
        this.put("numResults", 0);
        this.put("warning", "");
        this.put("error", "");
        this.put("resultType", "");
        this.put("result", new ArrayList<>());
    }


    public String getId() {
        return this.getString("id");
    }

    public void setId(String id) {
        this.put("id", id);
    }


    public Object getDBTime() {
        return this.get("dbTime");
    }

    public void setDBTime(int value) {
        this.put("dbTime", value);
    }


    public int getNumResults() {
        return this.getInt("numResults");
    }

    public void setNumResults(int value) {
        this.put("numResults", value);
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


    public int getResultType() {
        return this.getInt("resultType");
    }

    public void setResultType(Object value) {
        this.put("resultType", value);
    }


    public List getResult() {
        return (List) this.get("result");
    }

    public void setResult(List value) {
        this.put("result", value);
        this.setNumResults(value.size());
    }

}
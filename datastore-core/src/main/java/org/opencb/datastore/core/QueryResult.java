package org.opencb.datastore.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 20/03/14.
 */
public class QueryResult<T> {
    private String id;
    private int dbTime;
    private int numResults;
    private int numTotalResults;
    private String warningMsg;
    private String errorMsg;
    private String resultType;
    private List<T> result;


    public QueryResult() {
        this("", -1, -1, -1, "", "", new ArrayList<T>());
    }

    public QueryResult(String id) {
        this(id, -1, -1, -1, "", "", new ArrayList<T>());
    }

    public QueryResult(String id, int dbTime, int numResults, int numTotalResults, String warningMsg, String errorMsg, List<T> result) {
        this.id = id;
        this.dbTime = dbTime;
        this.numResults = numResults;
        this.numTotalResults = numTotalResults;
        this.warningMsg = warningMsg;
        this.errorMsg = errorMsg;
        this.resultType = result.size() > 0 ? result.get(0).getClass().getCanonicalName() : "";
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getDbTime() {
        return dbTime;
    }

    public void setDbTime(int dbTime) {
        this.dbTime = dbTime;
    }

    public int getNumResults() {
        return numResults;
    }

    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public List<T> getResult() {
        return result;
    }

    public void setResult(List<T> result) {
        if (result.size() > 0) {
            this.resultType = result.get(0).getClass().getCanonicalName();
        }
        this.numResults = result.size();
        this.result = result;
    }

    public void addResult(T result) {
        this.resultType = result.getClass().getCanonicalName();
        this.result.add(result);
        this.numResults = this.result.size();
    }

    public void addAllResults(List<T> result) {
        if (result.size() > 0) {
            this.resultType = result.get(0).getClass().getCanonicalName();
        }
        this.result.addAll(result);
        this.numResults = this.result.size();
    }

    @Override
    public String toString() {
        return "QueryResult{\n" +
                "id='" + id + '\'' + "\n" +
                ", dbTime=" + dbTime + "\n" +
                ", numResults=" + numResults + "\n" +
                ", warningMsg='" + warningMsg + '\'' + "\n" +
                ", errorMsg='" + errorMsg + '\'' + "\n" +
                ", resultType='" + resultType + '\'' + "\n" +
                ", result=" + result + "\n" +
                '}';
    }

    public int getNumTotalResults() {
        return numTotalResults;
    }

    public void setNumTotalResults(int numTotalResults) {
        this.numTotalResults = numTotalResults;
    }
}

package org.opencb.datastore.core;

import java.io.IOException;
import java.util.List;

/**
 * Created by jacobo on 5/11/14.
 */
public interface QueryResultWriter<T> {

    public void open() throws IOException;

    public void write(T elem) throws IOException;

    public void close() throws IOException;

}

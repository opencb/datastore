package org.opencb.datastore.core;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jacobo on 25/03/15.
 */
public class QueryOptionsTest {


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void addtoList() {
        QueryOptions options = new QueryOptions("include", "csv1,csv2,csv3");
        options.addToListOption("include", "value4");
        options.addToListOption("include", "value5");
        Assert.assertEquals(Arrays.asList("csv1", "csv2", "csv3", "value4", "value5"), options.get("include"));
    }

    @Test
    public void addtoList2() {
        QueryOptions options = new QueryOptions();
        options.addToListOption("include", "value1");
        options.addToListOption("include", "value2");
        options.addToListOption("include", "value3");
        Assert.assertEquals(Arrays.asList("value1", "value2", "value3"), options.get("include"));
    }

    @Test
    public void addtoList3() {
        QueryOptions options = new QueryOptions();
        options.addToListOption("include", "value1");
        options.addToListOption("include", 2);
        options.addToListOption("include", '3');
        Assert.assertEquals(Arrays.asList("value1", 2, '3'), options.get("include"));
    }

    @Test
    public void addtoList4() {
        List include = new ArrayList(2);
        include.add("value1");
        include.add("value2");
        QueryOptions options = new QueryOptions("include", Collections.unmodifiableList(include));
        thrown.expect(UnsupportedOperationException.class);
        options.addToListOption("include", "value1");
    }
}

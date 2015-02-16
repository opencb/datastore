package org.opencb.datastore.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by imedina on 24/03/14.
 */
public class ObjectMapTest {

    private ObjectMap objectMap;

    @Before
    public void setUp() throws Exception {
        objectMap = new ObjectMap();
        objectMap.put("string", "hello");
        objectMap.put("integer", 1);
        objectMap.put("long", 123_456_789_000L);
        objectMap.put("boolean", true);
        objectMap.put("float", 1.0f);
        objectMap.put("double", 1.0);
        ArrayList<String> list = new ArrayList<>();
        list.add("elem1");
        list.add("elem2");
        objectMap.put("list", list);
        objectMap.put("listCsv", "1,2,3,4,5");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        objectMap.put("map", map);

    }

    @Test
    public void testToJson() throws Exception {
        System.out.println(objectMap.toJson());
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        System.out.println(jsonObjectMapper.writeValueAsString(objectMap));

    }

    @Test
    public void testSafeToString() throws Exception {

    }

    @Test
    public void testGet() throws Exception {
        Integer s = (Integer) objectMap.get("integer");
        System.out.println(s);
    }

    @Test
    public void testGetString() throws Exception {
        assertEquals(objectMap.getString("string"), "hello");
        assertEquals(objectMap.getString("integer"), "1");
    }

    @Test
    public void testGetInt() throws Exception {
        assertEquals(objectMap.getInt("integer"), 1);
    }

    @Test
    public void testGetLong() throws Exception {
        assertEquals(objectMap.getLong("long"), 123_456_789_000L);
    }

    @Test
    public void testGetFloat() throws Exception {

    }

    @Test
    public void testGetDouble() throws Exception {

    }

    @Test
    public void testGetBoolean() throws Exception {

    }

    @Test
    public void testGetList() throws Exception {
        List<Object> list = objectMap.getList("list");
        System.out.println(list);
        System.out.println((String)list.get(0));
    }

    @Test
    public void testGetListAs() throws Exception {
        List<String> list = objectMap.getListAs("list", String.class);
        System.out.println(list);
        System.out.println(list.get(0));
    }

    @Test
    public void testGetAsList() throws Exception {
        List<String> list = objectMap.getAsStringList("list");
        assertEquals(list, objectMap.get("list"));
        assertEquals(list, objectMap.getAsList("list"));

        list = objectMap.getAsStringList("listCsv", ":");
        assertEquals(list.get(0), objectMap.getString("listCsv"));

        list = objectMap.getAsStringList("listCsv");
        assertEquals(list, Arrays.asList("1", "2", "3", "4", "5"));

        List<Integer> listCsv = objectMap.getAsIntegerList("listCsv");
        assertEquals(listCsv, Arrays.asList(1, 2, 3, 4, 5));

        list = objectMap.getAsStringList("unExisting");
        assertTrue(list.isEmpty());
    }

    @Test
    public void testGetMap() throws Exception {

    }
}

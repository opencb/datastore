package org.opencb.datastore.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by imedina on 20/03/14.
 */
public class ObjectMap implements Map<String, Object>, Serializable {

    private Map<String, Object> objectMap;
    protected ObjectMapper jsonObjectMapper = new ObjectMapper();

    public ObjectMap() {
        objectMap = new LinkedHashMap<>();
    }

    public ObjectMap(int size) {
        objectMap = new LinkedHashMap<>(size);
    }

    public ObjectMap(final String key, final Object value) {
        objectMap = new LinkedHashMap<>();
        objectMap.put(key, value);
    }

    public ObjectMap(final Map<String, Object> map) {
        objectMap = new LinkedHashMap<>(map);
    }

    public ObjectMap(String json) {
        try {
            objectMap = new LinkedHashMap<>();
            objectMap.putAll(jsonObjectMapper.readValue(json, objectMap.getClass()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String toJson() {
        try {
            return jsonObjectMapper.writeValueAsString(objectMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Deprecated
    public String safeToString() {
        Iterator<String> iter = objectMap.keySet().iterator();
        String key;
        StringBuilder sb = new StringBuilder("{\n");
        while(iter.hasNext()) {
            key = iter.next();
            if(!key.equals("result")) {
                sb.append("\t"+key+": " + objectMap.get(key)+",\n");
            }else {
//				sb.append("\t"+key+": " + objectMap.getString(key).substring(0, 10)+"...\n");
            }
        }
        sb.append("}");
        return sb.toString();
    }


    public String getString(String field) {
        return getString(field, "");
    }

    public String getString(String field, String defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            if(objectMap.get(field) != null) {
                return objectMap.get(field).toString();
            }else {
                return null;
            }
        }
        return defaultValue;
    }


    public int getInt(String field) {
        return getInt(field, 0);
    }

    public int getInt(String field, int defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            Object obj = objectMap.get(field);
            switch(obj.getClass().getSimpleName()) {
                case "Integer":
                    return (Integer)obj;
                case "Long":
                    return ((Long)obj).intValue();
                case "Double":
                    return ((Double)obj).intValue();
                case "Float":
                    return ((Float)obj).intValue();
                case "String":
                    return Integer.parseInt(String.valueOf(obj));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }

    public long getLong(String field) {
        return getLong(field, 0L);
    }

    public long getLong(String field, long defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            Object obj = objectMap.get(field);
            switch(obj.getClass().getSimpleName()) {
                case "Long":
                    return (Long)obj;
                case "Integer":
                    return ((Integer)obj).longValue();
                case "Double":
                    return ((Double)obj).longValue();
                case "Float":
                    return ((Float)obj).longValue();
                case "String":
                    return Long.parseLong(String.valueOf(obj));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public float getFloat(String field) {
        return getFloat(field, 0.0f);
    }

    public float getFloat(String field, float defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            Object obj = objectMap.get(field);
            switch(obj.getClass().getSimpleName()) {
                case "Float":
                    return (Float)obj;
                case "Double":
                    return ((Double)obj).floatValue();
                case "Integer":
                    return ((Integer)obj).floatValue();
                case "Long":
                    return ((Long)obj).floatValue();
                case "String":
                    return Float.parseFloat((String) objectMap.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public double getDouble(String field) {
        return getDouble(field, 0.0);
    }

    public double getDouble(String field, double defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            Object obj = objectMap.get(field);
            switch(obj.getClass().getSimpleName()) {
                case "Double":
                    return (Double)obj;
                case "Float":
                    return ((Float)obj).doubleValue();
                case "Integer":
                    return ((Integer)obj).doubleValue();
                case "Long":
                    return ((Long)obj).doubleValue();
                case "String":
                    return Double.parseDouble((String) objectMap.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public boolean getBoolean(String field) {
        return getBoolean(field, false);
    }

    public boolean getBoolean(String field, boolean defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            Object obj = objectMap.get(field);
            switch(obj.getClass().getSimpleName()) {
                case "Boolean":
                    return (Boolean) objectMap.get(field);
                case "String":
                    return Boolean.parseBoolean((String) objectMap.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    /**
     * Gets the value of the given key, casting it to the given {@code Class<T>}.  This is useful to avoid having casts
     * in client code, though the effect is the same.  So to get the value of a key that is of type String, you would write
     * {@code String name = doc.get("name", String.class)} instead of {@code String name = (String) doc.get("x") }.
     *
     * @param field   the field
     * @param clazz the class to cast the value to
     * @param <T>   the type of the class
     * @return the value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final Object field, final Class<T> clazz) {
        return (T) objectMap.get(field);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final Object field, final Class<T> clazz, T defaultvalue) {
        if(objectMap.containsKey(field)) {
            return (T) objectMap.get(field);
        }
        return defaultvalue;
    }

    /**
     * Some fields can be a List, this method cast the Object to List of generic Objects
     * @param field
     * @return A List representation of the field
     */
    public List<Object> getList(String field) {
        return getList(field, null);
    }

    public List<Object> getList(String field, final List<Object> defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            return (List<Object>) objectMap.get(field);
        }
        return defaultValue;
    }

    public List<String> getAsStringList(String field) {
        return getAsStringList(field, ",");
    }

    public List<String> getAsStringList(String field, String separator) {
        List list = getAsList(field, separator);
        if (!list.isEmpty() && list.get(0) instanceof String) {
            return ((List<String>) list);
        } else {
            List<String> stringList = new ArrayList<>(list.size());
            for (Object o : list) {
                stringList.add(o == null? null : o.toString());
            }
            return stringList;
        }
    }

    public List<Integer> getAsIntegerList(String field) {
        return getAsIntegerList(field, ",");
    }
    public List<Integer> getAsIntegerList(String field, String separator) {
        List list = getAsList(field, separator);
        if (!list.isEmpty() && list.get(0) instanceof Integer) {
            return ((List<Integer>) list);
        } else {
            List<Integer> integerList = new ArrayList<>(list.size());
            for (Object o : list) {
                int i;
                if (o instanceof Integer) {
                    i = (int) o;
                } else {
                    i = Integer.parseInt(o.toString());
                }
                integerList.add(i);
            }
            return integerList;
        }
    }

    /**
     * Some fields can be a List, this method cast the Object to aList of type T. Gets the value of the given key, casting it to the given {@code Class<T>}.  This is useful to avoid having casts
     * in client code, though the effect is the same.  So to get the value of a key that is of type String, you would write
     * {@code String name = doc.get("name", String.class)} instead of {@code String name = (String) doc.get("x") }.
     * @param field
     * @param clazz
     * @return A List representation of the field
     */
    @Deprecated
    public <T> List<T> getAsList(String field, final Class<T> clazz) {
        return getAsList(field, clazz, null);
    }

    @Deprecated
    public <T> List<T> getAsList(String field, final Class<T> clazz, List<T> defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            return (List<T>) objectMap.get(field);
        }
        return defaultValue;
    }

    public List<Object> getAsList(String field) {
        return getAsList(field, ",");
    }

    public List<Object> getAsList(String field, String separator) {
        Object value = get(field);
        if (value == null) {
            return Collections.emptyList();
        } else {
            if (value instanceof List) {
                return (List) value;
            } else {
                return Arrays.<Object>asList(value.toString().split(separator));
            }
        }
    }


    /**
     * Some fields can be also a Map, this method cast the Object to Map
     * @param field
     * @return A Map representation of the field
     */
    public Map<String, Object> getMap(String field) {
        return getMap(field, null);
    }


    public Map<String, Object> getMap(String field, Map<String, Object> defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            return (Map<String, Object>) objectMap.get(field);
        }
        return defaultValue;
    }

    /**
     * Some fields can be a Map, this method cast the Object to Map of type T. Gets the value of the given key, casting it to the given {@code Class<T>}.  This is useful to avoid having casts
     * in client code, though the effect is the same.  So to get the value of a key that is of type String, you would write
     * {@code String name = doc.get("name", String.class)} instead of {@code String name = (String) doc.get("x") }.
     * @param field
     * @return A Map representation of the field
     */
    @Deprecated
    public <T> Map<String, T> getMapAs(String field, final Class<T> clazz) {
        return getMapAs(field, clazz, null);
    }

    @Deprecated
    public <T> Map<String, T> getMapAs(String field, final Class<T> clazz, Map<String, T> defaultValue) {
        if(field != null && objectMap.containsKey(field)) {
            return (Map<String, T>) objectMap.get(field);
        }
        return defaultValue;
    }


    /**
     * Map methods implementation. Side effect of composition.
     */
    @Override
    public int size() {
        return objectMap.size();
    }

    @Override
    public boolean isEmpty() {
        return objectMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return objectMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return objectMap.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return objectMap.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return objectMap.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return objectMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        objectMap.putAll(m);
    }

    @Override
    public void clear() {
        objectMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return objectMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return objectMap.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return objectMap.entrySet();
    }
}

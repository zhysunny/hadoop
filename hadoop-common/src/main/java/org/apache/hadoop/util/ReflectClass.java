package org.apache.hadoop.util;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 反射配置类
 * @author 章云
 * @date 2019/6/20 11:16
 */
public class ReflectClass {
    public static Map<String, Object> reflect(Class<?> cls) {
        Field[] fields = cls.getFields();
        Class<?> type = null;
        String fieldName = null;
        Object fieldValue = null;
        Map<String, Object> map = new LinkedHashMap<String, Object>(fields.length);
        try {
            for (Field field : fields) {
                type = field.getType();
                fieldName = field.getName();
                if (type == boolean.class || type == Boolean.class) {
                    fieldValue = field.getBoolean(fieldName);
                } else {
                    fieldValue = field.get(fieldName);
                }
                map.put(fieldName, fieldValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }
}

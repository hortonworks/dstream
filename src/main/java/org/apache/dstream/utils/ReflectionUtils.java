package org.apache.dstream.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public class ReflectionUtils {
	
	public static <T> T newDefaultInstance(Class<T> clazz) {
		try {
			Constructor<T> ctr = clazz.getDeclaredConstructor();
			ctr.setAccessible(true);
			return ctr.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static void setFieldValue(Object instance, String fieldPath, Object newValue) {
		String[] parsedFieldPaths = fieldPath.split("\\.");
		Object result = instance;
		for (int i = 1; i < parsedFieldPaths.length; i++) {
			result = doGetFieldValue(result, parsedFieldPaths[i]);
		}
		
		try {
			Field field = result.getClass().getDeclaredField(parsedFieldPaths[parsedFieldPaths.length - 1]);
			field.setAccessible(true);
			field.set(result, newValue);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static Object doGetFieldValue(Object instance, String fieldName) {
		Field field = findField(instance.getClass(), fieldName, null);
		try {
			if (field != null) {
				field.setAccessible(true);
				return field.get(instance);
			} else {
				throw new NoSuchFieldException("Field '" + fieldName
						+ "' does not exists in " + instance);
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static Field findField(Class<?> clazz, String name, Class<?> type) {
		Assert.notNull(clazz, "Class must not be null");
		Assert.isTrue(name != null || type != null, "Either name or type of the field must be specified");
		Class<?> searchType = clazz;
		while (!Object.class.equals(searchType) && searchType != null) {
			Field[] fields = searchType.getDeclaredFields();
			for (Field field : fields) {
				if ((name == null || name.equals(field.getName()))
						&& (type == null || type.equals(field.getType()))) {
					return field;
				}
			}
			searchType = searchType.getSuperclass();
		}
		return null;
	}
}

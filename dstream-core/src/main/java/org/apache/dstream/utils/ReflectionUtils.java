package org.apache.dstream.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

public class ReflectionUtils {
	
	public static <T> T newDefaultInstance(Class<T> clazz) {
		try {
			Constructor<T> ctr = clazz.getDeclaredConstructor();
			ctr.setAccessible(true);
			return ctr.newInstance();
		} 
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T newDefaultInstance(String className) {
		try {
			Class<T> clazz = (Class<T>) Class.forName(className, false, Thread.currentThread().getContextClassLoader());
			return newDefaultInstance(clazz);
		} 
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static <T> T newInstance(Class<T> clazz, Class<?>[] argumentTypes,  Object[] arguments) {
		if (argumentTypes == null || argumentTypes.length < 1){
			return newDefaultInstance(clazz);
		}
		try {
			Constructor<T> ctr = clazz.getDeclaredConstructor(argumentTypes);
			ctr.setAccessible(true);
			return ctr.newInstance(arguments);
		} 
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String className, Class<?>[] argumentTypes,  Object[] arguments) {
		if (argumentTypes == null || argumentTypes.length < 1){
			return newDefaultInstance(className);
		}
		try {
			Class<T> clazz = (Class<T>) Class.forName(className, false, Thread.currentThread().getContextClassLoader());
			return newInstance(clazz, argumentTypes, arguments);
		} 
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static Method findMethod(Class<?> targetClass, Class<?> returnType, Class<?>... inputParams) throws Exception {
		Class<?> searchType = targetClass;
		while (searchType != null) {
			Method[] methods = (searchType.isInterface() ? searchType.getMethods() : searchType.getDeclaredMethods());
			for (Method method : methods) {
				if (method.getReturnType().isAssignableFrom(returnType)){
					Class<?>[] paramTypes = method.getParameterTypes();
					if (Arrays.equals(paramTypes, inputParams)){
						return method;
					}
				}
			}
			searchType = searchType.getSuperclass();
		}
		throw new NoSuchMethodException("Method which takes " + Arrays.asList(inputParams) + 
				" parameters and return " + returnType + 
				" is not found in object of class " + targetClass);
	}
	
	public static Method findMethod(String name, Class<?> clazz, Class<?> returnType, Class<?>... inputParams) throws Exception {
		Class<?> searchType = clazz;
		while (searchType != null) {
			Method[] methods = (searchType.isInterface() ? searchType.getMethods() : searchType.getDeclaredMethods());
			for (Method method : methods) {
				if (name.equals(method.getName()) &&
						(inputParams == null || Arrays.equals(inputParams, method.getParameterTypes()))) {
					return method;
				}
			}
			searchType = searchType.getSuperclass();
		}
		throw new NoSuchMethodException("Method which takes " + Arrays.asList(inputParams) + 
				" parameters and return " + returnType + 
				" is not found in object of class " + clazz);
	}

	public static void setFieldValue(Object instance, String fieldPath, Object newValue) {
		String[] parsedFieldPaths = fieldPath.split("\\.");
		Object result = instance;
		int fieldIndex = 0;
		for (int i = 1; i < parsedFieldPaths.length; i++) {
			result = doGetFieldValue(result, parsedFieldPaths[i]);
			fieldIndex++;
		}
		
		try {
			Field field = result.getClass().getDeclaredField(parsedFieldPaths[fieldIndex]);
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
	
	@SuppressWarnings("unchecked")
	public static <T> T getFieldValue(Object rootInstance, String fieldPath, Class<T> targetFieldType) {
		String[] parsedFieldPaths = fieldPath.split("\\.");
		Object result = rootInstance;
		int fieldIndex = 0;
		for (int i = 1; i < parsedFieldPaths.length; i++) {
			result = doGetFieldValue(result, parsedFieldPaths[i]);
			fieldIndex++;
		}
		
		try {	
			Field field = findField(result.getClass(), parsedFieldPaths[fieldIndex], targetFieldType);
			field.setAccessible(true);
			return (T) field.get(result);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}

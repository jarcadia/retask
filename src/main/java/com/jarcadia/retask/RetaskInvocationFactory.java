//package com.jarcadia.retask;
//
//import java.beans.BeanInfo;
//import java.beans.IntrospectionException;
//import java.beans.Introspector;
//import java.beans.PropertyDescriptor;
//import java.lang.reflect.Method;
//import java.lang.reflect.Parameter;
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//import com.fasterxml.jackson.databind.JavaType;
//import com.jarcadia.retask.RetaskProxyMetadata.Getter;
//import com.jarcadia.retask.annontations.RetaskProxy;
//
//public class RetaskInvocationFactory {
//	
//	public void analyze(List<HandlerMethod> handlers) {
//		// Scan all handler methods to detect @RetaskProxy classes
//        final Map<Class<?>, RetaskProxyMetadata> proxyMetadata = new HashMap<>();
//        for (HandlerMethod handler : handlers) {
//            for (Parameter param : handler.getMethod().getParameters()) {
//                RetaskProxy proxyAnnontation = param.getType().getAnnotation(RetaskProxy.class);
//                if (proxyAnnontation != null) {
//                    
//                    List<Getter> getters = new LinkedList<>();
//                    try {
//                        BeanInfo info = Introspector.getBeanInfo(param.getType());
//                        for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
//                            Method method = pd.getReadMethod();
//                            if (method != null) {
//                                JavaType getterReturnType = rcommando.getObjectMapper().constructType(method.getGenericReturnType());
//                                getters.add(new Getter(method, pd.getName(), getterReturnType));
//                            }
//                        }
//                        proxyMetadata.put(param.getType(), new RetaskProxyMetadata(getters));
//                    } catch (IntrospectionException e) {
//                        logger.warn("Unable to introspect @RetaskProxy " + param.getClass().getName());
//                    }
//                }
//            }
//        }
//	}
//
////	public RetaskInvocationHandler produceHandler(RcObject object, Class<?> clazz, Getters metadata) {
////		Map<Method, Object> values = new HashMap<>();
////		RcValues rcValues = object.get(metadata.getGetterFieldNames());
////		for (int i=0; i < metadata.getGetterFieldNames().length; i++) {
////			values.put(metadata.getMethods()[i], rcValues.next().as(metadata.getReturnTypes()[i]));
////		}
////		return new RetaskInvocationHandler(object, values);
////	}
//	
//
//}

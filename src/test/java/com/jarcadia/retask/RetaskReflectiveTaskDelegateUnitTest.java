package com.jarcadia.retask;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.data.TestPojo;

public class RetaskReflectiveTaskDelegateUnitTest {
    
    static RedisCommando rcommando = Mockito.mock(RedisCommando.class);
    static ObjectMapper objectMapper = new ObjectMapper();

    public void methodWithoutParameters() { }

    @Test
    void delegatesToMethodWithoutParametersCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithoutParameters");
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method);
        Object returnValue = delegate.invoke("taskName", "routingKey", 1, 1, null, null, "{}");
        Assertions.assertNull(returnValue);
    }

    public void methodWithParamsThatHaveReservedNames(String taskId, String routingKey, int attempt, int permit) {
        Assertions.assertEquals("taskId", taskId);
        Assertions.assertEquals("routingKey", routingKey);
        Assertions.assertEquals(2, attempt);
        Assertions.assertEquals(5, permit);
    }

    @Test
    void delegatesToMethodWithParamsThatHaveReservedNamedCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithParamsThatHaveReservedNames", String.class, String.class, int.class, int.class);
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method);
        Object returnValue = delegate.invoke("taskId", "routingKey", 2, 5, null, null, "{}");
        Assertions.assertNull(returnValue);
    }

    public void methodWithRedisMapParameters(@RetaskParam("cars") RedisMap carsMap, RedisMap books) {
        Assertions.assertEquals(carsMap.size(), 5);
        Assertions.assertEquals(books.size(), 10);
    }

    @Test
    void delegatesToMethodWithRedisMapParametersCorrectly() throws Throwable {
        // Setup mocks
        RedisMap carsMapMock = Mockito.mock(RedisMap.class);
        Mockito.when(carsMapMock.size()).thenReturn(5L);

        RedisMap booksMapMock = Mockito.mock(RedisMap.class);
        Mockito.when(booksMapMock.size()).thenReturn(10L);

        RedisCommando rcommando = Mockito.mock(RedisCommando.class);
        Mockito.when(rcommando.getMap("cars")).thenReturn(carsMapMock);
        Mockito.when(rcommando.getMap("books")).thenReturn(booksMapMock);

        Method method = this.getClass().getMethod("methodWithRedisMapParameters", RedisMap.class, RedisMap.class);
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method);
        Object returnValue = delegate.invoke("taskId", "routingKey", 1, -1, null, null, "{}");
        Assertions.assertNull(returnValue);
    }

    public void methodWithJsonParameters(String str, int number, List<String> strList, Set<Number> numSet, TestPojo pojo) {
        Assertions.assertEquals("hello", str);
        Assertions.assertEquals(42, number);
        Assertions.assertIterableEquals(Arrays.asList("hello", "world"), strList);
        Set<Integer> expectedSet = new HashSet<>();
        expectedSet.addAll(Arrays.asList(42, 44));
        Assertions.assertIterableEquals(numSet, expectedSet);
        TestPojo expectedPojo = new TestPojo();
        expectedPojo.setName("John Doe");
        expectedPojo.setAge(33);
    }

    @Test
    void delegatesToMethodWithJsonParametersCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithJsonParameters", String.class, int.class, List.class, Set.class, TestPojo.class);
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method);
        
        Map<String, Object> values = new HashMap<>();
        values.put("str", "hello");
        values.put("number", 42);
        values.put("strList", Arrays.asList("hello", "world"));
        Set<Integer> numSet = new HashSet<>();
        numSet.addAll(Arrays.asList(42, 44));
        values.put("numSet", numSet);
        TestPojo expectedPojo = new TestPojo();
        expectedPojo.setName("John Doe");
        expectedPojo.setAge(33);
        
        Object returnValue = delegate.invoke("taskId", "routingKey", 1, -1, null, null, objectMapper.writeValueAsString(values));
        Assertions.assertNull(returnValue);
    }

    public String methodWithReturnValue() {
        return "hello";
    }

    @Test
    void delegatesToMethodAndReceivesReturnValueCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithReturnValue");
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method);
        Object returnValue = delegate.invoke("taskName", "routingKey", 1, 1, null, null, "{}");
        Assertions.assertEquals("hello", returnValue);
    }

    public void changeHandlerMethod(RedisObject object, String before, @RetaskParam("after") TestPojo pojo) {
        Assertions.assertEquals("objs", object.getMapKey());
        Assertions.assertEquals("abc123", object.getId());
        Assertions.assertEquals("hello", before);
        Assertions.assertEquals("John Doe", pojo.getName());
        Assertions.assertEquals(33, pojo.getAge());
    }

    @Test
    void delegatesToChangeHandlerMethodCorrectly() throws Throwable {
        TestPojo pojo = new TestPojo();
        pojo.setName("John Doe");
        pojo.setAge(33);
        
        RedisObject objMock = Mockito.mock(RedisObject.class);
        Mockito.when(objMock.getMapKey()).thenReturn("objs");
        Mockito.when(objMock.getId()).thenReturn("abc123");

        RedisMap mapMock = Mockito.mock(RedisMap.class);
        Mockito.when(mapMock.get("abc123")).thenReturn(objMock);

        RedisCommando rcommando = Mockito.mock(RedisCommando.class);
        Mockito.when(rcommando.getMap("objs")).thenReturn(mapMock);

        Method method = this.getClass().getMethod("changeHandlerMethod", RedisObject.class, String.class, TestPojo.class);
        RetaskReflectiveTaskDelegate delegate = RetaskReflectiveTaskDelegate.createObjectHandlerDelegate(rcommando, objectMapper, c -> this, this.getClass(), method, "objs");
        delegate.invoke("taskName", "routingKey", 1, 1, objectMapper.writeValueAsString("hello"),
                objectMapper.writeValueAsString(pojo), objectMapper.writeValueAsString(Collections.singletonMap("objectId", "abc123")));
    }
}

package com.jarcadia.retask;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RcObject;
import com.jarcadia.rcommando.RcObjectMapper;
import com.jarcadia.rcommando.RcSet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.data.TestPojo;

public class RetaskReflectiveTaskDelegateUnitTest {
    
    static RedisCommando rcommando = Mockito.mock(RedisCommando.class);
    static ObjectMapper objectMapper = new RcObjectMapper(rcommando);

    public void methodWithoutParameters() { }
    
    @BeforeAll
    public static void setup() {
    	Mockito.when(rcommando.getObjectMapper()).thenReturn(new RcObjectMapper(rcommando));
    }

    @Test
    void delegatesToMethodWithoutParametersCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithoutParameters");
        
        
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        Object returnValue = delegate.invoke("taskName", "routingKey", 1, 1, null, null, "{}", new TaskBucket());
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
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        Object returnValue = delegate.invoke("taskId", "routingKey", 2, 5, null, null, "{}", new TaskBucket());
        Assertions.assertNull(returnValue);
    }

    public void methodWithMultipleRedisObjects(RcObject book, @RetaskParam("car") RcObject theCar, int count) {
        Assertions.assertNotNull(book);
        Assertions.assertNotNull(theCar);
        Assertions.assertEquals("books", book.getSetKey());
        Assertions.assertEquals("book1", book.getId());
        Assertions.assertEquals("cars", theCar.getSetKey());
        Assertions.assertEquals("car1", theCar.getId());
        Assertions.assertEquals(42, count);
    }

    @Test
    void delegateToMethodWithMultipleRedisObjectsProperly() throws Throwable {
    	// Setup mocks
    	RcObject bookMock = Mockito.mock(RcObject.class);
        Mockito.when(bookMock.getSetKey()).thenReturn("books");
        Mockito.when(bookMock.getId()).thenReturn("book1");

    	RcSet booksMapMock = Mockito.mock(RcSet.class);
    	Mockito.when(booksMapMock.size()).thenReturn(1L);
    	Mockito.when(booksMapMock.get("book1")).thenReturn(bookMock);
    	
    	RcObject carMock = Mockito.mock(RcObject.class);
        Mockito.when(carMock.getSetKey()).thenReturn("cars");
        Mockito.when(carMock.getId()).thenReturn("car1");

    	RcSet carsMapMock = Mockito.mock(RcSet.class);
    	Mockito.when(carsMapMock.size()).thenReturn(1L);
    	Mockito.when(carsMapMock.get("car1")).thenReturn(carMock);
    	
        Mockito.when(rcommando.getSetOf("books")).thenReturn(booksMapMock);
        Mockito.when(rcommando.getSetOf("cars")).thenReturn(carsMapMock);
    	
        Method method = this.getClass().getMethod("methodWithMultipleRedisObjects", RcObject.class, RcObject.class, int.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        Map<String, Object> params = new HashMap<>();
        params.put("count", 42);
        params.put("book", bookMock);
        params.put("car", carMock);
        
        delegate.invoke("taskName", "routingKey", 1, 1, null, null, objectMapper.writeValueAsString(params), new TaskBucket());
    }
    
    
    public void methodWithRcSetParameters(@RetaskParam("cars") RcSet carsMap, RcSet books) {
        Assertions.assertEquals(carsMap.size(), 5);
        Assertions.assertEquals(books.size(), 10);
    }

    @Test
    void delegatesToMethodWithRcSetParametersCorrectly() throws Throwable {
        // Setup mocks
        RcSet carsMapMock = Mockito.mock(RcSet.class);
        Mockito.when(carsMapMock.size()).thenReturn(5L);

        RcSet booksMapMock = Mockito.mock(RcSet.class);
        Mockito.when(booksMapMock.size()).thenReturn(10L);

        Mockito.when(rcommando.getSetOf("cars")).thenReturn(carsMapMock);
        Mockito.when(rcommando.getSetOf("books")).thenReturn(booksMapMock);

        Method method = this.getClass().getMethod("methodWithRcSetParameters", RcSet.class, RcSet.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        Object returnValue = delegate.invoke("taskId", "routingKey", 1, -1, null, null, "{}", new TaskBucket());
        Assertions.assertNull(returnValue);
    }

    public void methodWithJsonParameters(String str, int number, List<String> strList, Set<Number> numSet, TestPojo pojo) {
        Assertions.assertEquals("hello", str);
        Assertions.assertEquals(42, number);
        Assertions.assertIterableEquals(Arrays.asList("hello", "world"), strList);
        Set<Integer> expectedSet = new HashSet<>();
        expectedSet.addAll(Arrays.asList(42, 44));
        Assertions.assertIterableEquals(numSet, expectedSet);
        Assertions.assertEquals("John Doe", pojo.getName());
        Assertions.assertEquals(33, pojo.getAge());
    }

    @Test
    void delegatesToMethodWithJsonParametersCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithJsonParameters", String.class, int.class, List.class, Set.class, TestPojo.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        Set<Integer> numSet = new HashSet<>();
        numSet.addAll(Arrays.asList(42, 44));
        TestPojo pojo = new TestPojo();
        pojo.setName("John Doe");
        pojo.setAge(33);
        Map<String, Object> values = Map.of("str", "hello", "number", 42, "strList", Arrays.asList("hello", "world"), "numSet", numSet, "pojo", pojo);
        Object returnValue = delegate.invoke("taskId", "routingKey", 1, -1, null, null, objectMapper.writeValueAsString(values), new TaskBucket());
        Assertions.assertNull(returnValue);
    }
    
    public void methodWithCollectionOfPojos(List<TestPojo> pojos) {
    	Assertions.assertEquals(2, pojos.size());
    	Assertions.assertEquals(TestPojo.class, pojos.get(0).getClass());
    }

    @Test
    void delegatesToMethodWithCollectionOfPojosCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithCollectionOfPojos", List.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);

        TestPojo john = new TestPojo();
        john.setName("John Doe");
        john.setAge(33);
        TestPojo jane = new TestPojo();
        john.setName("Jane Doe");
        john.setAge(32);
        Map<String, Object> values = Map.of("pojos", List.of(john, jane));
        
        Object returnValue = delegate.invoke("taskId", "routingKey", 1, -1, null, null, objectMapper.writeValueAsString(values), new TaskBucket());
        Assertions.assertNull(returnValue);
    }

    public String methodWithReturnValue() {
        return "hello";
    }

    @Test
    void delegatesToMethodAndReceivesReturnValueCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithReturnValue");
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        Object returnValue = delegate.invoke("taskName", "routingKey", 1, 1, null, null, "{}", new TaskBucket());
        Assertions.assertEquals("hello", returnValue);
    }

    public void changeHandlerMethod(RcObject object, String before, @RetaskParam("after") TestPojo pojo) {
        Assertions.assertEquals("objs", object.getSetKey());
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
        
        RcObject objMock = Mockito.mock(RcObject.class);
        Mockito.when(objMock.getSetKey()).thenReturn("objs");
        Mockito.when(objMock.getId()).thenReturn("abc123");

        RcSet mapMock = Mockito.mock(RcSet.class);
        Mockito.when(mapMock.get("abc123")).thenReturn(objMock);

        Mockito.when(rcommando.getSetOf("objs")).thenReturn(mapMock);

        Method method = this.getClass().getMethod("changeHandlerMethod", RcObject.class, String.class, TestPojo.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        delegate.invoke("taskName", "routingKey", 1, 1, objectMapper.writeValueAsString("hello"),
                objectMapper.writeValueAsString(pojo), objectMapper.writeValueAsString(Map.of("object", Map.of("setKey", "objs", "id", "abc123"))), new TaskBucket());
    }
}

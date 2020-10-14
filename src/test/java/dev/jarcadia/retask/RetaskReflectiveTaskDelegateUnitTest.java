package dev.jarcadia.retask;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import dev.jarcadia.retask.data.TestPojo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JavaType;
import dev.jarcadia.redao.Dao;
import dev.jarcadia.redao.Index;
import dev.jarcadia.redao.DaoValue;
import dev.jarcadia.redao.DaoValues;
import dev.jarcadia.redao.RedaoCommando;
import dev.jarcadia.redao.RedaoObjectMapper;
import dev.jarcadia.redao.proxy.Proxy;
import dev.jarcadia.retask.annontations.RetaskParam;

public class RetaskReflectiveTaskDelegateUnitTest {
    
    static RedaoCommando rcommando = Mockito.mock(RedaoCommando.class);
    static RedaoObjectMapper objectMapper = new RedaoObjectMapper(rcommando);

    public void methodWithoutParameters() { }
    
    @BeforeAll
    public static void setup() {
    	objectMapper.registerProxyClass(TestProxy.class);
    	Mockito.when(rcommando.getObjectMapper()).thenReturn(objectMapper);
    }

    @Test
    void delegatesToMethodWithoutParametersCorrectly() throws Throwable {
        Method method = this.getClass().getMethod("methodWithoutParameters");
        
        
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
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
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        Object returnValue = delegate.invoke("taskId", "routingKey", 2, 5, null, null, "{}", new TaskBucket());
        Assertions.assertNull(returnValue);
    }

    public void methodWithMultipleRedisObjects(Dao book, @RetaskParam("car") Dao theCar, int count) {
        Assertions.assertNotNull(book);
        Assertions.assertNotNull(theCar);
        Assertions.assertEquals("books", book.getType());
        Assertions.assertEquals("book1", book.getId());
        Assertions.assertEquals("cars", theCar.getType());
        Assertions.assertEquals("car1", theCar.getId());
        Assertions.assertEquals(42, count);
    }

    @Test
    void delegateToMethodWithMultipleRedisObjectsProperly() throws Throwable {
    	// Setup mocks
    	Dao bookMock = Mockito.mock(Dao.class);
        Mockito.when(bookMock.getType()).thenReturn("books");
        Mockito.when(bookMock.getId()).thenReturn("book1");

    	Index booksMapMock = Mockito.mock(Index.class);
    	Mockito.when(booksMapMock.count()).thenReturn(1L);
    	Mockito.when(booksMapMock.get("book1")).thenReturn(bookMock);
    	
    	Dao carMock = Mockito.mock(Dao.class);
        Mockito.when(carMock.getType()).thenReturn("cars");
        Mockito.when(carMock.getId()).thenReturn("car1");

    	Index carsMapMock = Mockito.mock(Index.class);
    	Mockito.when(carsMapMock.count()).thenReturn(1L);
    	Mockito.when(carsMapMock.get("car1")).thenReturn(carMock);
    	
        Mockito.when(rcommando.getPrimaryIndex("books")).thenReturn(booksMapMock);
        Mockito.when(rcommando.getPrimaryIndex("cars")).thenReturn(carsMapMock);
    	
        Method method = this.getClass().getMethod("methodWithMultipleRedisObjects", Dao.class, Dao.class, int.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        Map<String, Object> params = new HashMap<>();
        params.put("count", 42);
        params.put("book", bookMock);
        params.put("car", carMock);
        
        delegate.invoke("taskName", "routingKey", 1, 1, null, null, objectMapper.writeValueAsString(params), new TaskBucket());
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
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
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
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
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
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        Object returnValue = delegate.invoke("taskName", "routingKey", 1, 1, null, null, "{}", new TaskBucket());
        Assertions.assertEquals("hello", returnValue);
    }

    public void changeHandlerMethod(Dao object, String before, @RetaskParam("after") TestPojo pojo) {
        Assertions.assertEquals("objs", object.getType());
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
        
        Dao objMock = Mockito.mock(Dao.class);
        Mockito.when(objMock.getType()).thenReturn("objs");
        Mockito.when(objMock.getId()).thenReturn("abc123");

        Index mapMock = Mockito.mock(Index.class);
        Mockito.when(mapMock.get("abc123")).thenReturn(objMock);

        Mockito.when(rcommando.getPrimaryIndex("objs")).thenReturn(mapMock);

        Method method = this.getClass().getMethod("changeHandlerMethod", Dao.class, String.class, TestPojo.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        delegate.invoke("taskName", "routingKey", 1, 1, objectMapper.writeValueAsString("hello"),
                objectMapper.writeValueAsString(pojo), objectMapper.writeValueAsString(Map.of("object", "objs/abc123")), new TaskBucket());
    }
    
    
    
    
    @Test
    void delegatesToChangeHandlerMethodUsingLenientMatchingCorrectly() throws Throwable {
        TestPojo pojo = new TestPojo();
        pojo.setName("John Doe");
        pojo.setAge(33);
        
        Dao objMock = Mockito.mock(Dao.class);
        Mockito.when(objMock.getType()).thenReturn("objs");
        Mockito.when(objMock.getId()).thenReturn("abc123");

        Index mapMock = Mockito.mock(Index.class);
        Mockito.when(mapMock.get("abc123")).thenReturn(objMock);

        Mockito.when(rcommando.getPrimaryIndex("objs")).thenReturn(mapMock);

        Method method = this.getClass().getMethod("changeHandlerMethod", Dao.class, String.class, TestPojo.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of());
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        delegate.invoke("taskName", "routingKey", 1, 1, objectMapper.writeValueAsString("hello"),
                objectMapper.writeValueAsString(pojo), objectMapper.writeValueAsString(Map.of("random", "objs/abc123")), new TaskBucket());
    }
    
    
    
    
    
    
    
    
    public interface TestProxy extends Proxy {
    	
    	public String getName();
    	public int getAge();

    }
    
    public void methodWithProxy(TestProxy object) {
    	Assertions.assertEquals("John Doe", object.getName());
    	Assertions.assertEquals(23, object.getAge());
    }

    @Test
    void delegatesToMethodWithProxy() throws Throwable {
    	
    	JavaType stringType = objectMapper.getTypeFactory().constructType(String.class);
    	JavaType intType = objectMapper.getTypeFactory().constructType(Integer.class);
    	
    	DaoValue nameMock = Mockito.mock(DaoValue.class);
    	Mockito.when(nameMock.as(stringType)).thenReturn("John Doe");

    	DaoValue ageMock = Mockito.mock(DaoValue.class);
    	Mockito.when(ageMock.as(intType)).thenReturn(23);

    	DaoValues valuesMock = setupValuesMock(nameMock, ageMock);

        Dao objMock = Mockito.mock(Dao.class);
        Mockito.when(objMock.getType()).thenReturn("objs");
        Mockito.when(objMock.getId()).thenReturn("abc123");
        Mockito.when(objMock.get(new String[] {"name", "age"})).thenReturn(valuesMock);

        Index mapMock = Mockito.mock(Index.class);
        Mockito.when(mapMock.get("abc123")).thenReturn(objMock);
        Mockito.when(rcommando.getPrimaryIndex("objs")).thenReturn(mapMock);
        
        TestProxy mockProxy = Mockito.mock(TestProxy.class);
        Mockito.when(mockProxy.getName()).thenReturn("John Doe");
        Mockito.when(mockProxy.getAge()).thenReturn(23);
        Mockito.when(objMock.as(TestProxy.class)).thenReturn(mockProxy);
        
        Method method = this.getClass().getMethod("methodWithProxy", TestProxy.class);
        ParamsProducer paramsProducer = new ParamsProducer(rcommando, Mockito.mock(Retask.class), method.getParameters(), Set.of(TestProxy.class));
        RetaskReflectiveTaskDelegate delegate = new RetaskReflectiveTaskDelegate(new AtomicReference<>(this), method, paramsProducer);
        
        delegate.invoke("taskName", "routingKey", 1, 1, null, null,
        		objectMapper.writeValueAsString(Map.of("object", "objs/abc123")), new TaskBucket());
    }
    
    private DaoValues setupValuesMock(DaoValue... values) {
    	DaoValues valuesMock = Mockito.mock(DaoValues.class);
    	Iterator<DaoValue> iter = List.of(values).iterator();
    	Mockito.when(valuesMock.iterator()).thenAnswer( i -> iter);
    	return valuesMock;
    }
}

//package dev.jarcadia;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dev.jarcadia.proxy.Proxy;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//
//import java.lang.invoke.MethodHandles;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class ProxyUnitTest {
//
//    static ObjectMapper mapper;
//    static ProxyFactory factory;
//    static ValueFormatter formatter;
//
//    @BeforeAll()
//    static void setup() {
//        mapper = new ObjectMapper();
//        factory = new ProxyFactory(mapper);
//        mapper.registerModule(new PersistJsonModule(type -> null));
//        formatter = new ValueFormatter(mapper);
//    }
//
//    @Test
//    void testGetType() {
//        Record record = mockRecord("obj", "abc123", Map.of());
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertEquals("obj", person.getType());
//    }
//
//    @Test
//    void testGetId() {
//        Record record = mockRecord("obj", "abc123", Map.of());
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertEquals("abc123", person.getId());
//    }
//
//    @Test
//    void testGetPath() {
//        Record record = mockRecord("obj", "abc123", Map.of());
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertEquals("obj/abc123", person.getPath());
//    }
//
//    @Test
//    void testMissingField() {
//        Record record = mockRecord("obj", "abc123", Map.of());
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertNull(person.getName());
//    }
//
//    @Test
//    void testGetStringField() {
//        Record record = mockRecord("obj", "abc123", Map.of("name", "John Doe"));
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertEquals("John Doe", person.getName());
//    }
//
//    @Test
//    void testGetListField() {
//        Record record = mockRecord("obj", "abc123", Map.of("nicknames", List.of("Jdizzle", "Jdog")));
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertIterableEquals(List.of("Jdizzle", "Jdog"), person.getNicknames());
//    }
//
//    @Test
//    void testGetEmptyOptional() {
//        Record record = mockRecord("obj", "abc123", Map.of());
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertTrue(person.getFax().isEmpty());
//    }
//
//    @Test
//    void testPopulatedOptional() {
//        Record record = mockRecord("obj", "abc123", Map.of("fax", "123-4567"));
//        PersonProxy person = factory.createObjectProxy(record, PersonProxy.class);
//        Assertions.assertTrue(person.getFax().isPresent());
//        Assertions.assertEquals("123-4567", person.getFax().get());
//    }
//
//
//
//
//
////
////        Assertions.assertEquals("abc123", person.getId());
////        Assertions.assertEquals(23, person.getAge());
////        Assertions.assertEquals("jd@test.com", person.getEmail().get());
////        Assertions.assertTrue(person.isEmployed());
////        Assertions.assertTrue(person.getFax().isEmpty());
////        Assertions.assertIterableEquals(List.of("Jdizzle", "Jdog"), person.getNicknames());
////        Assertions.assertEquals(100000, person.getSalary());
////        Assertions.assertTrue(person.needsRaise(10500));
////        Assertions.assertEquals(23, person.getRecord().get("age").asInt());
////        Assertions.assertEquals("111-22-3333", person.getSocialSecurityNumber());
////    }
//
////    @Test
////    void testProxySet() {
////        RecordUnitTest.PersonProxy proxy = record.as(RecordUnitTest.PersonProxy.class);
////        proxy.setName("Jane Doe");
////        proxy.setAge(24);
////        proxy.setIQ(145);
////        proxy.setSocialSecurityNumber("111-22-3333");
////        Assertions.assertEquals("Jane Doe", record.get("name").asString());
////        Assertions.assertEquals(24, record.get("age").asInt());
////        Assertions.assertEquals(145, record.get("iq").asInt());
////        Assertions.assertEquals("111-22-3333", record.get("socialSecurityNumber").asString());
////
////        Assertions.assertEquals("abc123", proxy.getId());
////        Assertions.assertEquals("Jane Doe", proxy.getName());
////        Assertions.assertEquals(24, proxy.getAge());
////        Assertions.assertEquals("111-22-3333", proxy.getSocialSecurityNumber());
////    }
////
////    @Test
////    void testProxyMultiSet() {
////        record.set("name", "Jane Smith", "fax", "123-4567");
////
////        RecordUnitTest.PersonProxy proxy = record.as(RecordUnitTest.PersonProxy.class);
////        proxy.setSomethingComplicated("Jane Doe", 24, 145, "111-22-3333");
////
////        Assertions.assertEquals("abc123", record.getId());
////        Assertions.assertEquals("Jane Doe", record.get("name").asString());
////        Assertions.assertEquals(24, record.get("age").asInt());
////        Assertions.assertEquals(145, record.get("IQ").asInt());
////        Assertions.assertEquals("111-22-3333", record.get("socialSecurityNumber").asString());
////        Assertions.assertFalse(record.get("fax").isPresent());
////
////        Assertions.assertEquals("abc123", proxy.getId());
////        Assertions.assertEquals("Jane Doe", proxy.getName());
////        Assertions.assertEquals(24, proxy.getAge());
////        Assertions.assertEquals("111-22-3333", proxy.getSocialSecurityNumber());
////        Assertions.assertTrue(proxy.getFax().isEmpty());
////    }
////
////    @Test
////    void testProxyEquals() {
////        RecordUnitTest.PersonProxy p1 = recordSet.get("abc123").as(RecordUnitTest.PersonProxy.class);
////        RecordUnitTest.PersonProxy p2 = recordSet.get("abc123").as(RecordUnitTest.PersonProxy.class);
////        Assertions.assertTrue(p1.equals(p2));
////    }
//
//    public interface PersonProxy extends Proxy {
//
//        static MethodHandles.Lookup createLookup() {
//            return MethodHandles.lookup();
//        }
//
//        String getName();
//
//        int getAge();
//
//        Optional<String> getEmail();
//
//        Optional<String> getFax();
//
//        List<String> getNicknames();
//
//        boolean isEmployed();
//
//        String getSocialSecurityNumber();
//
//        default int getSalary() {
//            return 100000;
//        }
//
//        default boolean needsRaise(int minimumSalary) {
//            return this.getSalary() >= getSalary();
//        }
//
//        void setName(String name);
//
//        void setAge(int age);
//
//        void setIQ(int iq);
//
//        void setSocialSecurityNumber(String socialSecurityNumber);
//
//        void setSomethingComplicated(String name, int age, int IQ, String socialSecurityNumber);
//    }
//
//    private Record mockRecord(String type, String id, Map<String, Object> data) {
//        Record record = Mockito.mock(Record.class);
//        Mockito.when(record.getType()).thenReturn(type);
//        Mockito.when(record.getId()).thenReturn(id);
//        Mockito.when(record.getPath()).thenReturn(type + "/" + id);
//        Mockito.when(record.getFields(Mockito.any())).then(answer -> {
//            String[] fields = Stream.of(answer.getArguments()).map(o -> (String) o)
//                    .collect(Collectors.toList()).toArray(new String[0]);
//            String[] vals = Stream.of(fields)
//                    .map(field -> data.containsKey(field) ? formatter.serialize(data.get(field)) : null)
//                    .collect(Collectors.toList()).toArray(new String[0]);
//            return Values.fromArrays(formatter, fields, vals);
//        });
//
//        return record;
//    }
//
//
//}

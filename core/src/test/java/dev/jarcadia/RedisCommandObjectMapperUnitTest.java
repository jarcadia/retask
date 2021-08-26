//package dev.jarcadia;
//
//import java.io.IOException;
//import java.util.Set;
//
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//@ExtendWith(MockitoExtension.class)
//public class RedisCommandObjectMapperUnitTest {
//
//	@Mock
//    Persistor rcommando;
//
//	@Mock
//    MemRecordSet recordSet;
//
//	@Mock
//    MemRecord record;
//
//	@BeforeEach
//	public void setupRcommando() {
//    	Mockito.when(rcommando.getRecordSet("objs")).thenReturn(recordSet);
//	}
//
//	@BeforeEach
//	public void setupDaoSet() {
//    	Mockito.when(recordSet.get("abc123")).thenReturn(record);
//	}
//
//	@BeforeEach
//	public void setupDao() {
//    	Mockito.when(record.getType()).thenReturn("objs");
//    	Mockito.when(record.getId()).thenReturn("abc123");
//		Mockito.when(record.getPath()).thenReturn("objs/abc123");
//	}
//
//    @Test
//    void basicDao() throws IOException {
//		RecordSetCache cache = new RecordSetCache();
//    	ObjectMapper mapper = new PersistObjectMapper(cache, Set.of());
//    	String serialized = mapper.writeValueAsString(this.record);
//    	Assertions.assertEquals(quoted("objs/abc123"), serialized);
//    	MemRecord record = mapper.readValue(serialized, MemRecord.class);
//    	Assertions.assertEquals("objs", record.getType());
//    	Assertions.assertEquals("abc123", record.getId());
//		Assertions.assertEquals("objs/abc123", record.getPath());
//	}
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//    private String quoted(String str) {
//    	return '"' + str + '"';
//    }
//}

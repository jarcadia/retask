package com.jarcadia.retask;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.jarcadia.retask.example.valid.ExampleWorkerAlpha;
import com.jarcadia.retask.example.valid.ExampleWorkerAlphaCopy;
import com.jarcadia.retask.example.valid.ExampleWorkerBeta;
import com.jarcadia.retask.example.valid.ExampleWorkerGamma;

public class RetaskWorkerDiscoveryServiceUnitTest {
    
    @Test
    void scanningClassesRecruitsExpectedHandlers() {
        RetaskRecruiter discoveryService = new RetaskRecruiter();
//        discoveryService.recruitFromPackage("com.jarcadia.retask.example.valid");
        discoveryService.recruitFromClass(ExampleWorkerAlpha.class);
        discoveryService.recruitFromClass(ExampleWorkerAlphaCopy.class);
        discoveryService.recruitFromClass(ExampleWorkerBeta.class);
        discoveryService.recruitFromClass(ExampleWorkerGamma.class);

        Map<String, Set<WorkerHandlerMethod>> result = discoveryService.getRecruits();
        Assertions.assertEquals(5, result.size());
        assertHelper(result.get("alpha").iterator(), ExampleWorkerAlpha.class, "handler");
        assertHelper(result.get("beta.one").iterator(), ExampleWorkerBeta.class, "handlerOne");
        assertHelper(result.get("beta.two").iterator(), ExampleWorkerBeta.class, "handlerTwo");
        assertHelper(result.get("gamma").iterator(), ExampleWorkerGamma.class, "handler");
        
        // Special handling for the duplicate routing key, sort for consistent results
        Iterator<WorkerHandlerMethod> sortedDupes = result.get("alpha.one").stream()
                .sorted((a, b) -> a.getWorkerClass().getSimpleName().compareTo(b.getWorkerClass().getSimpleName()))
                .collect(Collectors.toList()).iterator();
        
        assertHelper(sortedDupes, ExampleWorkerAlpha.class, "handlerOne");
        assertHelper(sortedDupes, ExampleWorkerAlphaCopy.class, "differentHandler");
    }

    @Test
    void scanningPackageRecruitsExpectedHandlers() {
        RetaskRecruiter discoveryService = new RetaskRecruiter();
        discoveryService.recruitFromPackage("com.jarcadia.retask.example.valid");
        Map<String, Set<WorkerHandlerMethod>> result = discoveryService.getRecruits();
        Assertions.assertEquals(5, result.size());
        assertHelper(result.get("alpha").iterator(), ExampleWorkerAlpha.class, "handler");
        assertHelper(result.get("beta.one").iterator(), ExampleWorkerBeta.class, "handlerOne");
        assertHelper(result.get("beta.two").iterator(), ExampleWorkerBeta.class, "handlerTwo");
        assertHelper(result.get("gamma").iterator(), ExampleWorkerGamma.class, "handler");
        
        // Special handling for the duplicate routing key, sort for consistent results
        Iterator<WorkerHandlerMethod> sortedDupes = result.get("alpha.one").stream()
                .sorted((a, b) -> a.getWorkerClass().getSimpleName().compareTo(b.getWorkerClass().getSimpleName()))
                .collect(Collectors.toList()).iterator();
        
        assertHelper(sortedDupes, ExampleWorkerAlpha.class, "handlerOne");
        assertHelper(sortedDupes, ExampleWorkerAlphaCopy.class, "differentHandler");
    }

    private void assertHelper(Iterator<WorkerHandlerMethod> dwm, Class<?> expectedClass, String expectedMethodName) {
        WorkerHandlerMethod next = dwm.next();
        Assertions.assertEquals(expectedClass, next.getWorkerClass());
        Assertions.assertEquals(expectedMethodName, next.getMethod().getName());
    }
}
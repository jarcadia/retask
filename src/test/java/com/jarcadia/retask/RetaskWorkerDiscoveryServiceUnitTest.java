package com.jarcadia.retask;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(ExampleWorkerAlpha.class);
        recruiter.recruitFromClass(ExampleWorkerAlphaCopy.class);
        recruiter.recruitFromClass(ExampleWorkerBeta.class);
        recruiter.recruitFromClass(ExampleWorkerGamma.class);

        RecruitmentResults results = recruiter.recruit();

        Map<String, List<HandlerMethod<?>>> handlers = results.getHandlersByRoutingKey();
        Assertions.assertEquals(5, handlers.size());
        assertHelper(handlers.get("alpha").iterator(), ExampleWorkerAlpha.class, "handler");
        assertHelper(handlers.get("beta.one").iterator(), ExampleWorkerBeta.class, "handlerOne");
        assertHelper(handlers.get("beta.two").iterator(), ExampleWorkerBeta.class, "handlerTwo");
        assertHelper(handlers.get("gamma").iterator(), ExampleWorkerGamma.class, "handler");
        
        // Special handling for the duplicate routing key, sort for consistent results
        Iterator<HandlerMethod<?>> sortedDupes = handlers.get("alpha.one").stream()
                .sorted((a, b) -> a.getWorkerClass().getSimpleName().compareTo(b.getWorkerClass().getSimpleName()))
                .collect(Collectors.toList()).iterator();
        
        assertHelper(sortedDupes, ExampleWorkerAlpha.class, "handlerOne");
        assertHelper(sortedDupes, ExampleWorkerAlphaCopy.class, "differentHandler");
    }

    @Test
    void scanningPackageRecruitsExpectedHandlers() {
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromPackage("com.jarcadia.retask.example.valid");
        RecruitmentResults results = recruiter.recruit();
        Map<String, List<HandlerMethod<?>>> handlers = results.getHandlersByRoutingKey();
        Assertions.assertEquals(5, handlers.size());
        assertHelper(handlers.get("alpha").iterator(), ExampleWorkerAlpha.class, "handler");
        assertHelper(handlers.get("beta.one").iterator(), ExampleWorkerBeta.class, "handlerOne");
        assertHelper(handlers.get("beta.two").iterator(), ExampleWorkerBeta.class, "handlerTwo");
        assertHelper(handlers.get("gamma").iterator(), ExampleWorkerGamma.class, "handler");
        
        // Special handling for the duplicate routing key, sort for consistent results
        Iterator<HandlerMethod<?>> sortedDupes = handlers.get("alpha.one").stream()
                .sorted((a, b) -> a.getWorkerClass().getSimpleName().compareTo(b.getWorkerClass().getSimpleName()))
                .collect(Collectors.toList()).iterator();
        
        assertHelper(sortedDupes, ExampleWorkerAlpha.class, "handlerOne");
        assertHelper(sortedDupes, ExampleWorkerAlphaCopy.class, "differentHandler");
    }

    private void assertHelper(Iterator<HandlerMethod<?>> dwm, Class<?> expectedClass, String expectedMethodName) {
        HandlerMethod<?> next = dwm.next();
        Assertions.assertEquals(expectedClass, next.getWorkerClass());
        Assertions.assertEquals(expectedMethodName, next.getMethod().getName());
    }
}
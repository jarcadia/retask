package com.jarcadia.retask;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockitoSession;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.jarcadia.rcommando.exception.RedisCommandoException;

import io.lettuce.core.RedisCommandInterruptedException;

@ExtendWith(MockitoExtension.class)
public class RetaskPopperUnitTest {

    final ExecutorService executor = Executors.newCachedThreadPool();
    
    @Mock
    RetaskTaskPopperDao dao;

    @Mock
    RetaskProcrastinator procrastinator;
    
    @Test
    void taskPopperStartsAndStops() throws TimeoutException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        taskPopper.start();
        taskPopper.close();
        taskPopper.join(100, TimeUnit.MILLISECONDS);
    }
    
    @Test
    void taskPopperStopsCorrectlyWhenJoiningThreadIsInterrupted() throws TimeoutException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        taskPopper.start();
        taskPopper.close();
        Thread.currentThread().interrupt();
        taskPopper.join(1000, TimeUnit.MILLISECONDS);
    }
    
    @Test
    void taskPopperStopsCorrectlyWhenInterruptedWhileAwaitingPop() throws TimeoutException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        taskPopper.start();
        Mockito.when(dao.popTask()).thenThrow(new RedisCommandInterruptedException(new InterruptedException()));
        taskPopper.join(1000, TimeUnit.MILLISECONDS);
    }
    
    @Test
    void taskPopperStopsCorrectlyWhenInterruptedWhileAwaitingExceptionHandlingDelay() throws TimeoutException, InterruptedException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        Mockito.when(dao.popTask()).thenThrow(new RedisCommandoException("Generally failed to pop task"));
        doThrow(new InterruptedException()).when(procrastinator).sleepFor(1000L);
        taskPopper.start();
        taskPopper.join(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void taskPopperContinuesAfterDelayIfItEncountersAnUnexpectedException() throws TimeoutException, InterruptedException, ExecutionException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        Mockito.when(dao.popTask()).thenThrow(new RedisCommandoException("Generally failed to pop task"));
        taskPopper.start();
        Mockito.verify(dao, Mockito.timeout(Duration.ofMillis(100)).atLeastOnce()).popTask();
        Mockito.verify(procrastinator, Mockito.atLeastOnce()).sleepFor(1000L);
        taskPopper.close();
        taskPopper.join(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void taskPopperContinuesIfNoTasksAreAvailable() throws TimeoutException, InterruptedException {
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, (task, metadata) -> {}, procrastinator);
        taskPopper.start();
        Mockito.verify(dao, Mockito.timeout(Duration.ofMillis(100)).atLeastOnce()).popTask();
        taskPopper.close();
        taskPopper.join(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void taskPopperContinuesWhenTaskHandlerThrowsAnException() throws TimeoutException, InterruptedException, ExecutionException {
        // Expected data
        String task = "testTask";
        Map<String, String> metadata = Collections.singletonMap("hello", "world");

        // Setup TaskHandler
        CompletableFuture<String> taskFuture = new CompletableFuture<>();
        CompletableFuture<Map<String, String>> metadataFuture = new CompletableFuture<>();
        RawTaskHandler handler = (t, m) -> {
            taskFuture.complete(t);
            metadataFuture.complete(m); 
            throw new RuntimeException("Bad things happened");
        };
        
        // Mock the dao
        Mockito.when(dao.popTask()).thenReturn(task);
        Mockito.when(dao.getTaskMetadata(task)).thenReturn(metadata);

        // Setup TaskPopper for test
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, handler, procrastinator);
        taskPopper.start();

        // Assert values were passed into handler
        Assertions.assertEquals(task, taskFuture.get(100, TimeUnit.MILLISECONDS));
        Assertions.assertIterableEquals(metadata.entrySet(), metadataFuture.get().entrySet());

        // Close and join TaskPopper
        taskPopper.close();
        taskPopper.join(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void taskPopperWorksForSingleTask() throws InterruptedException, ExecutionException, TimeoutException {
        // Setup expected data
        String task = "testTaskAbc";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("greeting", "hello world");
        metadata.put("saluation", "goodbye world");

        // Setup TaskHandler
        CompletableFuture<String> taskFuture = new CompletableFuture<>();
        CompletableFuture<Map<String, String>> metadataFuture = new CompletableFuture<>();
        RawTaskHandler handler = (t, m) -> {
            taskFuture.complete(t);
            metadataFuture.complete(m); 
        };

        // Mock the dao
        Mockito.when(dao.popTask()).thenReturn(task);
        Mockito.when(dao.getTaskMetadata(task)).thenReturn(metadata);

        // Setup TaskPopper for test
        RetaskTaskPopper taskPopper = new RetaskTaskPopper(dao, executor, handler, procrastinator);
        taskPopper.start();

        Assertions.assertEquals(task, taskFuture.get(100, TimeUnit.MILLISECONDS));
        Assertions.assertIterableEquals(metadata.entrySet(), metadataFuture.get().entrySet());

        taskPopper.close();
        taskPopper.join(100, TimeUnit.MILLISECONDS);
    }
}

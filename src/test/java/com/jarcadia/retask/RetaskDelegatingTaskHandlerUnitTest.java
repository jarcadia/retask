package com.jarcadia.retask;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RetaskDelegatingTaskHandlerUnitTest {
    
    AtomicBoolean executed = new AtomicBoolean();
    
    @BeforeEach
    public void resetExecuted() {
        executed.set(false);
    }
    
    private RetaskDelegate createVerifyingWrapper(String task, String routingKey, int attempt, int permit, String changedId, String before, String after, Object returnValue) {
        return (receivedTask, receivedRoutingKey, receivedAttempt, receivedPermit, receivedBefore, receivedAfter, receivedParams) -> {
            Assertions.assertEquals(receivedTask, task);
            Assertions.assertEquals(receivedRoutingKey, routingKey);
            Assertions.assertEquals(receivedAttempt, attempt);
            Assertions.assertEquals(receivedPermit, permit);
            Assertions.assertEquals(receivedBefore, before);
            Assertions.assertEquals(receivedAfter, after);
            executed.set(true);
            return returnValue;
        };
    }

    private RetaskDelegatingTaskHandler createHandler(RetaskDao helper, RetaskDelegate delegate) {
        return new RetaskDelegatingTaskHandler(helper, delegate, new RetaskProcrastinator());
    }

    private RetaskDelegatingTaskHandler createHandler(RetaskDao helper, RetaskDelegate delegate, RetaskProcrastinator procrastinator) {
        return new RetaskDelegatingTaskHandler(helper, delegate, procrastinator);
    }

    private void assertDelegateInvoked() {
        Assertions.assertTrue(executed.get(), "Task was executed");
    }
    
    private void assertDelegateNotInvoked() {
        Assertions.assertFalse(executed.get(), "Task was not executed");
    }

    @Test
    void taskMethodParametersAreCorrect() throws Throwable {
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, null);

        RetaskDao helper = Mockito.mock(RetaskDao.class);
        RetaskDelegatingTaskHandler handler = createHandler(helper, delegate);
        
        Retask task = Retask.create("routingKey");
        handler.handle("abc", task.getMetadata());
        
        assertDelegateInvoked();
    }
    
    @Test
    void taskRequestsRetryCorrectly() throws Throwable {
        RetaskDelegate delegate = (receivedTask, receivedRoutingKey, receivedAttempt, receivedPermit, receivedBefore, receivedAfter, receivedParams) -> {
            Assertions.assertEquals(receivedTask, "abc");
            Assertions.assertEquals(receivedRoutingKey, "routingKey");
            executed.set(true);
            throw new RetaskRetryException(5, TimeUnit.SECONDS);
        };
        
        Retask task = Retask.create("routingKey");

        RetaskDao helper = Mockito.mock(RetaskDao.class);

        // Invoke the task
        RetaskDelegatingTaskHandler handler = createHandler(helper, delegate);
        handler.handle("abc", task.getMetadata());
        
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).retry("abc", 5000);
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void attemptIsIncrementedCorrectly() throws Throwable {
        
        Retask task = Retask.create("routingKey");
        Map<String, String> metadata = task.getMetadata();
        metadata.put("attempt", "2");

        RetaskDao helper = Mockito.mock(RetaskDao.class);
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 3, -1, null, null, null, null);

        // Invoke the task
        RetaskDelegatingTaskHandler handler = createHandler(helper, delegate);
        handler.handle("abc", task.getMetadata());
        
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void taskRecursCorrectlyWhenAuthorityIsPresent() throws Throwable {
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, null);
        
        Retask task = Retask.create("routingKey").recurEvery("test-recur", 1, TimeUnit.SECONDS);
        String generatedAuthKey = task.getAuthorityKey();

        RetaskDao helper = Mockito.mock(RetaskDao.class);
        Mockito.when(helper.recur("test-recur", "abc", generatedAuthKey, 500L, 1000L)).thenReturn(true);

        RetaskProcrastinator procrastinator = Mockito.mock(RetaskProcrastinator.class);
        Mockito.when(procrastinator.getCurrentTimeMillis()).thenReturn(500L);

        // Add additional metadata (automatically done by Lua script)
        Map<String, String> metadata = task.getMetadata();
        metadata.put("recurKey", task.getRecurKey());
        metadata.put("authorityKey", task.getAuthorityKey());
        
        // Invoke the task
        RetaskDelegatingTaskHandler handler = createHandler(helper, delegate, procrastinator);
        handler.handle("abc", metadata);

        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).recur("test-recur", "abc", generatedAuthKey, 500, 1000L);
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verifyNoMoreInteractions(helper);
    }

    @Test
    void doesNotRecurWhenLackingAuthority() throws Throwable {
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, null);

        Retask task = Retask.create("test").recurEvery("test-recur", 1, TimeUnit.SECONDS);
        String generatedAuthKey = task.getAuthorityKey();

        RetaskDao helper = Mockito.mock(RetaskDao.class);
        Mockito.when(helper.recur("test-recur", "abc", generatedAuthKey, 0, 1000L)).thenReturn(false);

        RetaskProcrastinator procrastinator = Mockito.mock(RetaskProcrastinator.class);
        Mockito.when(procrastinator.getCurrentTimeMillis()).thenReturn(500L);
        
        // Add additional metadata (automatically done by Lua script)
        Map<String, String> metadata = task.getMetadata();
        metadata.put("recurKey", task.getRecurKey());
        metadata.put("authorityKey", task.getAuthorityKey());

        // Invoke the task
        createHandler(helper, delegate, procrastinator).handle("abc", metadata);
        
        assertDelegateNotInvoked();
        Mockito.verify(helper, Mockito.times(1)).recur("test-recur", "abc", generatedAuthKey, 500L, 1000L);
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void runsAfterSucessfullyAcquiringPermit() throws Throwable {
        RetaskDelegate wrapper = createVerifyingWrapper("abc", "routingKey", 1, 2, null, null, null, null);
        Retask task = Retask.create("routingKey").permit("permitKey");

        // Create mocked helper to acquire permit 2
        RetaskDao helper = Mockito.mock(RetaskDao.class);
        Mockito.when(helper.acquirePermitOrBacklog("abc", "permitKey")).thenReturn(Optional.of(2));

        // Invoke the task
        createHandler(helper, wrapper).handle("abc", task.getMetadata());
        
        // Assertions
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).acquirePermitOrBacklog("abc", "permitKey");
        Mockito.verify(helper, Mockito.times(1)).releasePermit("permitKey", 2);
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void doesNotRunWhenAPermitIsUnavailable() throws Throwable {
        RetaskDelegate wrapper = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, null);
        Retask task = Retask.create("routingKey").permit("permitKey");

        // Create mocked helper to reject permit
        RetaskDao helper = Mockito.mock(RetaskDao.class);
        Mockito.when(helper.acquirePermitOrBacklog("abc", "permitKey")).thenReturn(Optional.empty());

        // Invoke the task
        createHandler(helper, wrapper).handle("abc", task.getMetadata());

        // Assertions
        assertDelegateNotInvoked();
        Mockito.verify(helper, Mockito.times(1)).acquirePermitOrBacklog("abc", "permitKey");
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void singleRetaskReturnValueIsSubmitted() throws Throwable {
        RetaskDao helper = Mockito.mock(RetaskDao.class);

        Retask task = Retask.create("routingKey");
        Retask returnedTask = Retask.create("returned");
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, returnedTask);

        // Invoke the task
        createHandler(helper, delegate).handle("abc", task.getMetadata());
        
        // Assertions
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verify(helper, Mockito.times(1)).submit(returnedTask);
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void retaskArrayReturnValueIsSubmitted() throws Throwable {
        RetaskDao helper = Mockito.mock(RetaskDao.class);

        Retask task = Retask.create("routingKey");
        Retask[] returnedTasks = new Retask[] {Retask.create("returnedOne"), Retask.create("returnedTwo")};
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, returnedTasks);

        // Invoke the task
        createHandler(helper, delegate).handle("abc", task.getMetadata());
        
        // Assertions
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verify(helper, Mockito.times(1)).submit(returnedTasks);
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void retaskListReturnValueIsSubmitted() throws Throwable {
        RetaskDao helper = Mockito.mock(RetaskDao.class);

        Retask task = Retask.create("routingKey");
        Retask returnedTask = Retask.create("returnedOne");
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, Collections.singletonList(returnedTask));

        // Invoke the task
        createHandler(helper, delegate).handle("abc", task.getMetadata());
        
        // Assertions
        assertDelegateInvoked();
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verify(helper, Mockito.times(1)).submit(returnedTask);
        Mockito.verifyNoMoreInteractions(helper);
    }
    
    @Test
    void taskWillSleepUntilTargetTimestamp() throws Throwable {
        RetaskDao helper = Mockito.mock(RetaskDao.class);

        long targetTimestamp = System.currentTimeMillis() + 10000;
        Retask task = Retask.create("routingKey").at(targetTimestamp);
        Retask returnedTask = Retask.create("returnedOne");
        RetaskDelegate delegate = createVerifyingWrapper("abc", "routingKey", 1, -1, null, null, null, Collections.singletonList(returnedTask));

        // Mock Sleeper
        RetaskProcrastinator procrastinator = Mockito.mock(RetaskProcrastinator.class);
        
        // Add additional metadata (automatically done by Lua script)
        Map<String, String> metadata = task.getMetadata();
        metadata.put("targetTimestamp", String.valueOf(targetTimestamp));

        // Invoke the task
        RetaskDelegatingTaskHandler handler = new RetaskDelegatingTaskHandler(helper, delegate, procrastinator);
        handler.handle("abc", metadata);
        
        // Assertions
        assertDelegateInvoked();
        Mockito.verify(procrastinator, Mockito.times(1)).sleepUntil(targetTimestamp);
        Mockito.verify(helper, Mockito.times(1)).clearParams("abc");
        Mockito.verify(helper, Mockito.times(1)).submit(returnedTask);
        Mockito.verifyNoMoreInteractions(helper);
    }
}

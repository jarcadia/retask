package com.jarcadia.retask;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RetaskScheduledTaskPollerUnitTest {

    @Mock
    RetaskRepository dao;

    @Mock
    RetaskProcrastinator procrastinator;


    @Test
    void startsAndStopsCorrectly() throws TimeoutException {
        RetaskScheduledTaskPoller poller = new RetaskScheduledTaskPoller(dao, procrastinator);
        poller.start();
        Mockito.verify(dao, Mockito.timeout(Duration.ofMillis(100)).atLeastOnce()).pollForScheduledTasks(ArgumentMatchers.anyLong());
        poller.close();
        poller.join(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void queuesTasksThatAreReady() throws TimeoutException {
        RetaskScheduledTaskPoller poller = new RetaskScheduledTaskPoller(dao, procrastinator);
        poller.start();
        List<String> readyTasks = Arrays.asList("task1", "task2");
        Mockito.when(dao.pollForScheduledTasks(ArgumentMatchers.anyLong())).thenReturn(readyTasks).thenReturn(Collections.emptyList());
        Mockito.verify(dao, Mockito.timeout(Duration.ofMillis(100)).atLeastOnce()).queueTaskIds(readyTasks);
        poller.close();
        poller.join(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void exitsGracefullyWhenInterrupted() throws TimeoutException, InterruptedException {
        // Setup mocks before starting the poller
        Mockito.when(procrastinator.getCurrentTimeMillis()).thenReturn(0L);
        Mockito.doThrow(new InterruptedException()).when(procrastinator).sleepFor(ArgumentMatchers.anyLong());

        // Start the poller
        RetaskScheduledTaskPoller poller = new RetaskScheduledTaskPoller(dao, procrastinator);
        poller.start();

        // Join without closing, the interrupted exception should have exited the polling thread
        poller.join(1000, TimeUnit.MILLISECONDS);

        // Verify polling was called once before the interrupted exception was thrown
        Mockito.verify(dao, Mockito.times(1)).pollForScheduledTasks(ArgumentMatchers.anyLong());
    }

}

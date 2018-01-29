package org.apache.rya.streams.querymanager;

import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

public class QueryManagerTest {
    private static final Scheduler TEST_SCHEDULER = Scheduler.newFixedRateSchedule(0, 100, TimeUnit.MILLISECONDS);
    private static Logger LOG;

    @BeforeClass
    public static void setupLogger() {
        org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender());
        LOG = LoggerFactory.getLogger(QueryManagerTest.class);
        LOG.info("test");
    }

    /**
     * Tests when the query manager is notified to create a new query, the query
     * is created and started.
     */
    @Test
    public void testCreateQuery() throws Exception {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final String ryaInstance = "ryaTestInstance";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        Mockito.when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(Matchers.eq(ryaInstance), Matchers.eq(query));
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive()));
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
            queryStarted.await(5, TimeUnit.SECONDS);
            Mockito.verify(qe).startQuery(ryaInstance, query);
        } finally {
            qm.stopAndWait();
        }
    }

    /**
     * Tests when the query manager is notified to delete a new query, the query
     * is stopped and deleted.
     */
    @Test
    public void testDeleteQuery() throws Exception {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true);
        final String ryaInstance = "ryaTestInstance";

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        Mockito.when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        final CountDownLatch queryDeleted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            queryDeleted.countDown();
            return null;
        }).when(qe).stopQuery(query.getQueryId());
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        Mockito.doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(Matchers.eq(ryaInstance), Matchers.eq(query));

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        // add the query, so it can be removed
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            Thread.sleep(1000);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive()));
            queryStarted.await(5, TimeUnit.SECONDS);
            newChangeLog.write(QueryChange.delete(query.getQueryId()));
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
            queryDeleted.await(5, TimeUnit.SECONDS);
            Mockito.verify(qe).stopQuery(query.getQueryId());
        } finally {
            qm.stopAndWait();
        }
    }

    /**
     * Tests when the query manager is notified to update an existing query, the
     * query is stopped.
     */
    @Test
    public void testUpdateQuery() throws Exception {
        // The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true);
        final String ryaInstance = "ryaTestInstance";

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        Mockito.when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        final CountDownLatch queryDeleted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            queryDeleted.countDown();
            return null;
        }).when(qe).stopQuery(query.getQueryId());
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        Mockito.doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(Matchers.eq(ryaInstance), Matchers.eq(query));

        // When the QueryChangeLogSource is subscribed to in the QueryManager,
        // mock notify of a new QueryChangeLog
        // add the query, so it can be removed
        Mockito.doAnswer(invocation -> {
            // The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            Thread.sleep(1000);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive()));
            queryStarted.await(5, TimeUnit.SECONDS);
            newChangeLog.write(QueryChange.update(query.getQueryId(), false));
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
            queryDeleted.await(10, TimeUnit.SECONDS);
            Mockito.verify(qe).stopQuery(query.getQueryId());
        } finally {
            qm.stopAndWait();
        }
    }
}

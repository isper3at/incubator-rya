package org.apache.rya.streams.querymanager;

import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChangeLog.QueryChangeLogException;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

public class QueryManagerTest {
    private static final Scheduler TEST_SCHEDULER = Scheduler.newFixedRateSchedule(0, 100, TimeUnit.MILLISECONDS);

    @BeforeClass
    public static void setupLogger() {
    }

    //Test no query change logs, add query change log.
    @Test
    public void testAddQueryChangeLog() throws Exception {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final String ryaInstance = "ryaTestInstance";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true);

        //mocks
        final QueryExecutor qe = mock(QueryExecutor.class);
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
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive()));
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
            queryStarted.await(5, TimeUnit.SECONDS);
            Mockito.verify(qe).startQuery("ryaTestInstance", query);
        } finally {
            qm.stopAndWait();
        }
    }

    // Test No query change logs, add a new one

    // Test existing query change log, add a new one with same rya instance
    @Test
    public void testAddQueryChangeLog_existing() {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();

        //mocks
        final QueryExecutor qe = mock(QueryExecutor.class);
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            //this should not work?
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
        } finally {
            qm.stopAndWait();
        }
        //assert log outputs "Discovered new Query Change Log for Rya Instance ryaTestInstance within source " + log.toString()
    }

    // Test No query change log, remove one
    @Test
    public void testRemoveQueryChangeLog_noneExist() {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();

        //mocks
        final QueryExecutor qe = mock(QueryExecutor.class);
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyDelete("ryaTestInstance");
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
        } finally {
            qm.stopAndWait();
        }
        //assert log outputs "Discovered new Query Change Log for Rya Instance ryaTestInstance within source " + log.toString()
    }

    // Test one existing query change log, remove one

    // Test existing query change log sources, remove one




    // Test no existing query delete query
    @Test
    public void testRemoveQuery_noneExist() {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();

        //mocks
        final QueryExecutor qe = mock(QueryExecutor.class);
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            //this should not work?
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
        } finally {
            qm.stopAndWait();
        }
        //assert log outputs "Discovered new Query Change Log for Rya Instance ryaTestInstance within source " + log.toString()
    }

    // Test no existing query update query

    // Test no existing query add query
    @Test
    public void testAddQuery() {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();

        //mocks
        final QueryExecutor qe = mock(QueryExecutor.class);
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        Mockito.doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate("ryaTestInstance", newChangeLog);
            return null;
        }).when(source).subscribe(Matchers.any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, TEST_SCHEDULER);
        try {
            qm.startAndWait();
            qe.startAndWait();

            //at this point, a query repository exists that is watching the in memory query change log.
            //  have the query change log register a create query.

            newChangeLog.write(QueryChange.create(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true));

        } catch (final QueryChangeLogException e) {
            e.printStackTrace();
        } finally {
            qm.stopAndWait();
            qe.stopAndWait();
        }
        //assert log outputs "Discovered new Query Change Log for Rya Instance ryaTestInstance within source " + log.toString()
    }

    // Test existing query add query

    // Test existing stopped query update to start

    // Test existing stopped query update to stop

    // Test existing started query update to start

    // Test existing started query update to stop

    // Test existing query delete
}

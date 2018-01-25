/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.rya.streams.querymanager;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChange.ChangeType;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChangeLogListener;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.apache.rya.streams.querymanager.QueryExecutor.QueryExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A service for managing {@link StreamsQuery} running on a Rya Streams system.
 * <p>
 * Only one QueryManager needs to be running to manage any number of rya
 * instances/rya streams instances.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManager extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    private final QueryExecutor queryExecutor;
    private final Scheduler scheduler;

    /**
     * Map of Rya Instance name to {@link QueryRepository}.
     */
    private final Map<String, QueryRepository> queryRepos;

    private final QueryChangeLogSource source;

    private final ReentrantLock lock;

    /**
     * Creates a new {@link QueryManager}.
     *
     * @param queryExecutor - The {@link QueryExecutor} that starts/creates
     *        queries when a CREATED query is found in the
     *        {@link QueryChangeLog}. (not null)
     * @param source - The {@link QueryChangeLogSource} of QueryChangeLogs. (not null)
     * @param scheduler - The {@link Scheduler} to use throughout
     */
    public QueryManager(final QueryExecutor queryExecutor, final QueryChangeLogSource source, final Scheduler scheduler) {
        this.source = requireNonNull(source);
        this.queryExecutor = requireNonNull(queryExecutor);
        this.scheduler = requireNonNull(scheduler);

        // subscribe to the repos and sources to be notified of changes.
        this.source.subscribe(new QueryManagerSourceListener());

        queryRepos = new HashMap<>();

        lock = new ReentrantLock();
    }

    /**
     * Starts running a query.
     *
     * @param query - The query to run.(not null)
     */
    private void runQuery(final StreamsQuery query) {
        requireNonNull(query);
        LOG.trace("Starting Query: " + query.getSparql());
        try {
            queryExecutor.startQuery("some name", query);
        } catch (final QueryExecutorException e) {
            LOG.error("Failed to start query.", e);
        }
    }

    /**
     * Stops the specified query from running.
     *
     * @param queryId - The ID of the query to stop running. (not null)
     */
    private void stopQuery(final UUID queryId) {
        requireNonNull(queryId);

        LOG.trace("Stopping query: " + queryId.toString());
        if (!queryExecutor.isRunning()) {
            throw new IllegalStateException("Query Executor must be started before queries can be managed.");
        }

        try {
            queryExecutor.stopQuery(queryId);
        } catch (final QueryExecutorException e) {
            LOG.error("Failed to stop query.", e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        LOG.trace("Starting Query Manager.");
        source.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.trace("Stopping Query Manager.");
        queryRepos.forEach((instance, repo) -> {
            try {
                queryExecutor.stopAll(instance);
            } catch (final QueryExecutorException e) {
                e.printStackTrace();
            }
            repo.stopAndWait();
        });
        queryExecutor.stopAndWait();
        source.stopAndWait();
    }

    /**
     * An implementation of {@link QueryChangeLogListener} for the
     * {@link QueryManager}.
     * <p>
     * When notified of a {@link ChangeType} performs one of the following:
     * <li>{@link ChangeType#CREATE}: Creates a new query using the
     * {@link QueryExecutor} provided to the {@link QueryManager}</li>
     * <li>{@link ChangeType#DELETE}: Deletes a running query by stopping the
     * {@link QueryExecutor} service of the queryID in the event</li>
     * <li>{@link ChangeType#UPDATE}: If the query is running and the update is
     * to stop the query, stops the query. Otherwise, if the query is not
     * running, it is removed.</li>
     */
    private class QueryManagerQueryChange implements QueryChangeLogListener {
        @Override
        public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent, final Optional<StreamsQuery> newQueryState) {
            LOG.debug("New query change event.");
            final QueryChange entry = queryChangeEvent.getEntry();

            switch (entry.getChangeType()) {
                case CREATE:
                    LOG.debug("Creating query event.");
                    if (newQueryState.isPresent()) {
                        runQuery(newQueryState.get());
                        LOG.trace("Created new query: " + newQueryState.get().toString());
                    }
                    break;
                case DELETE:
                    LOG.debug("delete query event.");
                    if (newQueryState.isPresent()) {
                        stopQuery(newQueryState.get().getQueryId());
                        LOG.trace("Deleted query: " + newQueryState.get().toString());
                    } else {
                        LOG.debug("Delete requested a query that does not exist yet.");
                    }
                    break;
                case UPDATE:
                    LOG.debug("update query event.");
                    if (!newQueryState.isPresent()) {
                        LOG.debug("Query: " + entry.getQueryId() + " does not exist yet, cannot perform update.");
                    } else {
                        final StreamsQuery updateQuery = newQueryState.get();
                        // if the query is currently inactive, and is updated to
                        // be active, turn on.
                        if (!updateQuery.isActive() && entry.getIsActive().or(false)) {
                            final StreamsQuery newQuery = new StreamsQuery(entry.getQueryId(), entry.getSparql().get(),
                                    entry.getIsActive().or(false));
                            runQuery(newQuery);
                            LOG.trace("Starting query: " + newQuery.toString());
                        } else if (!updateQuery.isActive() && !entry.getIsActive().or(true)) {
                            // if the query is running and the update turns it
                            // off, turn off.
                            stopQuery(updateQuery.getQueryId());
                            LOG.trace("Stopping query: " + updateQuery.toString());
                        } else {
                            LOG.debug("The query is either already running and "
                                    + "updated to turn on, or is already stopped and is updated to stop running.");
                        }
                    }
                    break;
            }
        }
    }

    /**
     * Listener used by the {@link QueryManager} to be notified when
     * {@link QueryChangeLog}s are created or deleted.
     */
    private class QueryManagerSourceListener implements SourceListener {
        @Override
        public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
            LOG.debug("Discovered new Query Change Log for Rya Instance " + ryaInstanceName + " within source " + log.toString());
            final QueryRepository repo = new InMemoryQueryRepository(log, scheduler);
            repo.startAndWait();
            repo.subscribe(new QueryManagerQueryChange());
            LOG.debug("New query repository started");
            queryRepos.put(ryaInstanceName, repo);
        }

        @Override
        public void notifyDelete(final String ryaInstanceName) {
            try {
                LOG.debug("Notified of deleting QueryChangeLog, stopping all queries belonging to the change log.");
                queryExecutor.stopAll(ryaInstanceName);
            } catch (final QueryExecutorException e) {
                LOG.error("Failed to stop all queries belonging to: " + ryaInstanceName, e);
            }
        }
    }
}

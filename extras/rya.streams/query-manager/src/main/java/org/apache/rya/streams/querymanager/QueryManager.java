/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.streams.querymanager;

import static java.util.Objects.requireNonNull;

import java.util.Set;
import java.util.UUID;

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

import com.google.common.util.concurrent.AbstractIdleService;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 *
 */
@DefaultAnnotation(NonNull.class)
public class QueryManager extends AbstractIdleService {
    private final String ryaInstanceName;
    private final QueryExecutor queryExecutor;
    private final Set<QueryChangeLogSource> sources;
    private final Set<QueryRepository> queryRepos;

    private final Set<StreamsQuery> queries;

    /**
     * Creates a new {@link QueryManager}.
     *
     * @param ryaInstanceName - The Rya Instance to connect to. (not null)
     * @param queryExecutor - The {@link QueryExecutor} that starts/creates queries
     *   when a CREATED query is found in the {@link QueryChangeLog}. (not null)
     * @param sources - The {@link QueryChangeLogSource}s the sources of QueryChangeLogs. (not null)
     * @param queryRepos - The set of {@link QueryRepository}s that will notify this manager
     *   when {@link ChangeLogEntry}s occur. (not null)
     */
    public QueryManager(final String ryaInstanceName, final QueryExecutor queryExecutor, final Set<QueryChangeLogSource> sources, final Set<QueryRepository> queryRepos) {
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        this.sources = requireNonNull(sources);
        this.queryExecutor = requireNonNull(queryExecutor);
        this.queryRepos = requireNonNull(queryRepos);

        queryRepos.forEach(repo -> repo.subscribe(new QueryManagerQueryChange()));
        sources.forEach(source -> source.subscribe(new QueryManagerSourceListener()));

        queries = new HashSet<>();
    }

    /**
     *
     *
     * @param query
     */
    private void runQuery(final StreamsQuery query) {
        if(!queryExecutor.isRunning()) {
            queryExecutor.startAndWait();
        }

        try {
            queryExecutor.startQuery(ryaInstanceName, query);
        } catch (final QueryExecutorException e) {
            e.printStackTrace();
        }

    }

    /**
     *
     */
    private void updateQuery() {

    }

    /**
     * @param queryId
     */
    private void stopQuery(final UUID queryId) {

    }


    @Override
    protected void startUp() throws Exception {
        queryRepos.forEach(repo -> repo.startAndWait());
        sources.forEach(source -> source.startAndWait());
    }

    @Override
    protected void shutDown() throws Exception {
        queryRepos.forEach(repo -> repo.stop());
        sources.forEach(source -> source.stop());
    }

    /**
     * An implementation of {@link QueryChangeLogListener} for the {@link QueryManager}.
     * <p>
     * When notified of a {@link ChangeType} performs one of the following:
     * <li>{@link ChangeType#CREATE}: Creates a new query using the {@link QueryExecutor} provided to the {@link QueryManager}</li>
     * <li>{@link ChangeType#DELETE}: Deletes a running query by stopping the {@link QueryExecutor} service of the queryID in the event</li>
     * <li>{@link ChangeType#UPDATE}: </li>
     */
    private class QueryManagerQueryChange implements QueryChangeLogListener {
        @Override
        public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent) {
            final QueryChange entry = queryChangeEvent.getEntry();
            switch(entry.getChangeType()) {
                case CREATE:
                    if (entry.getIsActive().or(false)) {
                        final StreamsQuery newQuery = new StreamsQuery(entry.getQueryId(), entry.getSparql().get(), entry.getIsActive().or(false));
                        runQuery(newQuery);
                    }
                    break;
                case DELETE:
                    //if was active, delete
                    break;
                case UPDATE:
                    if (entry.getIsActive().or(false)) {
                        final StreamsQuery newQuery = new StreamsQuery(entry.getQueryId(), entry.getSparql().get(), entry.getIsActive().or(false));
                        runQuery(newQuery);
                    } else {
                        //remove
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /**
     *
     */
    private class QueryManagerSourceListener implements SourceListener {
        @Override
        public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
            final QueryRepository repo = new InMemoryQueryRepository(log, scheduler);
            repo.subscribe(new QueryManagerQueryChange());
            repo.startAndWait();
        }

        @Override
        public void notifyDelete(final String ryaInstanceName) {
            try {
                queryExecutor.stopAll(ryaInstanceName);
            } catch (final QueryExecutorException e) {
                e.printStackTrace();
            }
        }
    }
}

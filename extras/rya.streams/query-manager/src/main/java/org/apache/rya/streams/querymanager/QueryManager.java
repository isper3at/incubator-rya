/**
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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.DeleteQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultAddQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultDeleteQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Used to manage {@link StreamsQuery}s in Rya Streams.
 *
 * The query manager looks for changes in the
 * {@link QueryChangeLog} and when a {@link QueryChange}
 * occurs, the query manager makes the appropriate change
 * to the {@link StreamsQuery}.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManager {
    private final Set<StreamsQuery> managedQueries;
    private final QueryChangeLog changeLog;
    private final QueryRepository queryRepo;

    /**
     * Creates a new {@link QueryManager}.
     */
    public QueryManager() {
        managedQueries = new HashSet<>();
        changeLog = new InMemoryQueryChangeLog();
        queryRepo = new InMemoryQueryRepository(changeLog, Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS));
    }

    private void startQuery(final String query) {
        System.out.println("Startings query: " + query);
        final AddQuery addQuery = new DefaultAddQuery(queryRepo);
    }

    private void stopQuery(final UUID queryId) {
        System.out.println("Stopping query: " + queryId.toString());
        final DeleteQuery stopQuery = new DefaultDeleteQuery(queryRepo);
        try {
            stopQuery.delete(queryId);
        } catch (final RyaStreamsException e) {
            e.printStackTrace();
        }
    }

    /**
     * Shuts down this {@link QueryManager}.
     */
    public void shutdown() {
        managedQueries.clear();
        try {
            System.out.print("Closing Query Repository....");
            queryRepo.stop();
            System.out.print("closed.\nClosing Query Change Log....");
            changeLog.close();
            System.out.print("closed.\n");
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}

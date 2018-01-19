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

import java.util.Set;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.openrdf.model.Statement;

import com.google.common.util.concurrent.Service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents the system that is responsible for ensuring active {@link StreamsQuery}s are
 * being processed.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryExecutor extends Service {
    /**
     * Starts running a {@link StreamsQuery}.
     *
     * @param ryaInstanceName - The rya instance whose {@link Statement}s will be processed by the query. (not null)
     * @param query - The query to run. (not null)
     */
    public void startQuery(final String ryaInstanceName, final StreamsQuery query);

    /**
     * Stops a {@link StreamsQuery}.
     *
     * @param queryID - The ID of the query to stop. (not null)
     */
    public void stopQuery(final UUID queryID);

    /**
     * @return - A set of {@link UUID}s representing the current active queries.
     */
    public Set<UUID> getRunningQueryIds();
}

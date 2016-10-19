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
package org.apache.rya.export.accumulo.policy;

import java.util.Date;
import java.util.Iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.api.conf.policy.TimestampPolicyStatementStore;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;

import mvm.rya.api.domain.RyaStatement;

/**
 * A {@link RyaStatementStore} decorated to connect to an Accumulo database and
 * filter statements based on a timestamp.
 */
public class TimestampPolicyAccumuloRyaStatementStore extends TimestampPolicyStatementStore {

    /**
     * Creates a new {@link TimestampPolicyAccumuloRyaStatementStore}
     * @param store
     * @param timestamp
     */
    public TimestampPolicyAccumuloRyaStatementStore(final AccumuloRyaStatementStore store, final Date timestamp) {
        super(store, timestamp);
        store.addIterator(getStartTimeSetting(timestamp));
    }

    /**
     * Creates an {@link IteratorSetting} with a time stamp filter that starts with the specified data.
     * @param time the start time of the filter.
     * @return the {@link IteratorSetting}.
     */
    private static IteratorSetting getStartTimeSetting(final Date time) {
        final IteratorSetting setting = new IteratorSetting(1, "startTimeIterator", TimestampFilter.class);
        TimestampFilter.setStart(setting, time.getTime(), true);
        TimestampFilter.setEnd(setting, Long.MAX_VALUE, true);
        return setting;
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() throws FetchStatementException {
        return store.fetchStatements();
    }
}

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
package org.apache.rya.mongodb.iter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.aggregation.AggregationUtil;
import org.bson.Document;
import org.openrdf.query.BindingSet;

import com.google.common.collect.Multimap;
import com.mongodb.AggregationOptions;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;

import info.aduna.iteration.CloseableIteration;

public class RyaStatementBindingSetCursorIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {
    private static final Logger log = Logger.getLogger(RyaStatementBindingSetCursorIterator.class);

    private final MongoCollection<Document> coll;
    private final Multimap<DBObject, BindingSet> rangeMap;
    private final Iterator<DBObject> queryIterator;
    private Long maxResults;
    private Iterator<Document> resultsIterator;
    private RyaStatement currentStatement;
    private Collection<BindingSet> currentBindingSetCollection;
    private Iterator<BindingSet> currentBindingSetIterator;
    private final MongoDBStorageStrategy<RyaStatement> strategy;
    private final Authorizations auths;

    public RyaStatementBindingSetCursorIterator(final MongoCollection<Document> coll,
            final Multimap<DBObject, BindingSet> rangeMap, final MongoDBStorageStrategy<RyaStatement> strategy,
            final Authorizations auths) {
        this.coll = coll;
        this.rangeMap = rangeMap;
        queryIterator = rangeMap.keySet().iterator();
        this.strategy = strategy;
        this.auths = auths;
    }

    @Override
    public boolean hasNext() {
        if (!currentBindingSetIteratorIsValid()) {
            findNextResult();
        }
        return currentBindingSetIteratorIsValid();
    }

    @Override
    public Entry<RyaStatement, BindingSet> next() {
        if (!currentBindingSetIteratorIsValid()) {
            findNextResult();
        }
        if (currentBindingSetIteratorIsValid()) {
            final BindingSet currentBindingSet = currentBindingSetIterator.next();
            return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(currentStatement, currentBindingSet);
        }
        return null;
    }

    private boolean currentBindingSetIteratorIsValid() {
        return (currentBindingSetIterator != null) && currentBindingSetIterator.hasNext();
    }

    private void findNextResult() {
        if (!currentResultCursorIsValid()) {
            findNextValidResultCursor();
        }
        if (currentResultCursorIsValid()) {
            // convert to Rya Statement
            final Document queryResult = resultsIterator.next();
            final DBObject dbo = (DBObject) JSON.parse(queryResult.toJson());
            currentStatement = strategy.deserializeDBObject(dbo);
            currentBindingSetIterator = currentBindingSetCollection.iterator();
        }
    }

    private void findNextValidResultCursor() {
        while (queryIterator.hasNext()){
            final DBObject currentQuery = queryIterator.next();
            currentBindingSetCollection = rangeMap.get(currentQuery);
            // Executing redact aggregation to only return documents the user
            // has access to.
            final List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$match", currentQuery));
            pipeline.addAll(AggregationUtil.createRedactPipeline(auths));
            log.debug(pipeline);

            final AggregationOptions opts = AggregationOptions.builder()
                .batchSize(1000)
                .build();
            final AggregateIterable<Document> aggIter = coll.aggregate(pipeline);
            resultsIterator = aggIter.iterator();
            if (resultsIterator.hasNext()) {
                break;
            }
        }
    }

    private boolean currentResultCursorIsValid() {
        return (resultsIterator != null) && resultsIterator.hasNext();
    }


    public void setMaxResults(final Long maxResults) {
        this.maxResults = maxResults;
    }

    @Override
    public void close() throws RyaDAOException {
        // TODO don't know what to do here
    }

    @Override
    public void remove() throws RyaDAOException {
        next();
    }

}

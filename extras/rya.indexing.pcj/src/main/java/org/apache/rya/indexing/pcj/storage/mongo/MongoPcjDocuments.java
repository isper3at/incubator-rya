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
package org.apache.rya.indexing.pcj.storage.mongo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.master.state.ClosableIterator;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.openrdf.query.BindingSet;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

/**
 *
 */
public class MongoPcjDocuments {
    private static final String PCJ_COLLECTION_NAME = "pcjs";
    private static final String VAR_ORDER_FIELD = "var_orders";
    private static final String SPARQL_FIELD = "sparql";
    private static final String PCJ_RESULTS_FIELD = "results";
    private static final String PCJ_ID = "_id";

    private final MongoCollection<Document> pcjCollection;

    public MongoPcjDocuments(final MongoClient client, final String ryaInstanceName) {
        requireNonNull(client);
        requireNonNull(ryaInstanceName);
        pcjCollection = client.getDatabase(ryaInstanceName).getCollection(PCJ_COLLECTION_NAME);
    }

    /**
     * Creates a new PCJ based on the provided metadata.  The initial pcj results will be empty.
     * @param pcjName - The unique name of the PCJ.
     * @param varOrders - The {@link VariableOrder}s.
     * @param sparql - The query the pcj is assigned to.
     */
    public void createPcj(final String pcjName, final Set<VariableOrder> varOrders, final String sparql) {
        final List<ImmutableList<String>> varDoc = new ArrayList<>();
        for (final VariableOrder order : varOrders) {
            varDoc.add(order.getVariableOrders());
        }
        final Document pcjDoc = new Document()
            .append(PCJ_ID, pcjName)
            .append(SPARQL_FIELD, sparql)
            .append(VAR_ORDER_FIELD, varDoc);
        pcjCollection.insertOne(pcjDoc);
    }

    /**
     * Gets the {@link PcjMetadata} from a provided PCJ name.
     *
     * @param pcjName
     * @return
     */
    public PcjMetadata getPcjMetadata(final String pcjName) {
        // since query by ID, there will only be one.
        final Document result = pcjCollection.find(new Document(PCJ_ID, pcjName)).first();
        final String sparql = result.getString(SPARQL_FIELD);
        final List<?> results = (List<?>) result.get(PCJ_RESULTS_FIELD);
        final int cardinality = result.size();
        final List<VariableOrder> varOrders = (List<VariableOrder>) result.get(VAR_ORDER_FIELD);
        return new PcjMetadata(sparql, cardinality, varOrders);
    }

    public void addResults(final String pcjName,
            final Collection<VisibilityBindingSet> results) {
        final Bson query = new Document(PCJ_ID, pcjName);
        final Document update = new Document("$push", makePushEachDoc(results));
        pcjCollection.findOneAndUpdate(query, update);
    }

    private Document makePushEachDoc(final Collection<VisibilityBindingSet> results) {
        return new Document();
    }

    public void purgePcjTable(final String pcjName) {
        final Bson query = new Document(PCJ_ID, pcjName);
        final Document update = new Document("$unset", new Document(PCJ_RESULTS_FIELD, ""));
        pcjCollection.findOneAndUpdate(query, update);
    }

    public CloseableIterator<BindingSet> listResults(final String pcjName, final Authorizations myAuths) {
        final FindIterable<Document> rez = pcjCollection.find(new Document(PCJ_ID, pcjName));

        // since lookup by ID, there should only be one.
        final Document doc = rez.first();
        final List<Bson> docBindingSets = (List<Bson>) doc.get(PCJ_RESULTS_FIELD);
        final Iterator<Bson> varOrderIter = ((List<Bson>) doc.get(VAR_ORDER_FIELD)).iterator();
        final Iterator<Bson> iter = docBindingSets.iterator();
        final ClosableIterator<BindingSet> bindingSets;
        return new CloseableIterator<BindingSet>() {
            final VisibilityBindingSetStringConverter ADAPTER = new VisibilityBindingSetStringConverter();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public BindingSet next() {
                final String bs = iter.next().toString();
                final VariableOrder varOrder = new VariableOrder(varOrderIter.next().toString());
                return ADAPTER.convert(bs, varOrder);
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    /**
     * Drops a pcj based on the PCJ name. Removing the entire document from
     * Mongo.
     *
     * @param pcjName
     *            - The identifier for the PCJ to remove.
     */
    public void dropPcjTable(final String pcjName) {
        pcjCollection.deleteOne(new Document(PCJ_ID, pcjName));
    }
}
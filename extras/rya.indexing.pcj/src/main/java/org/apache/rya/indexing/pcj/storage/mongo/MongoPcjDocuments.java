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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;

/**
 * Creates and modifies PCJs in MongoDB. PCJ's are stored as follows:
 *
 * <pre>
 * <code>
 * ----- PCJ Metadata Doc -----
 * {
 *   _id: [table_name]_METADATA,
 *   sparql: [sparql query to match results],
 *   cardinality: [number of results]
 * }
 *
 * ----- PCJ Results Doc -----
 * {
 *   pcjName: [table_name],
 *   auths: [auths]
 *   [binding_var1]: {
 *     uri: [type_uri],
 *     value: value
 *   }
 *   .
 *   .
 *   .
 *   [binding_varn]: {
 *     uri: [type_uri],
 *     value: value
 *   }
 * }
 * </code>
 * </pre>
 */
public class MongoPcjDocuments {
    public static final String PCJ_COLLECTION_NAME = "pcjs";

    // metadata fields
    public static final String CARDINALITY_FIELD = "cardinality";
    public static final String SPARQL_FIELD = "sparql";
    public static final String PCJ_ID = "_id";
    public static final String VAR_ORDER_ID = "varOrders";

    // pcj results fields
    private static final String BINDING_VALUE = "value";
    private static final String BINDING_TYPE = "uri";
    private static final String AUTHS_FIELD = "auths";
    private static final String PCJ_NAME = "pcjName";

    private final MongoCollection<Document> pcjCollection;
    private static final PcjVarOrderFactory pcjVarOrderFactory = new ShiftVarOrderFactory();

    /**
     * Creates a new {@link MongoPcjDocuments}.
     * @param client - The {@link MongoClient} to use to connect to mongo.
     * @param ryaInstanceName - The rya instance to connect to.
     */
    public MongoPcjDocuments(final MongoClient client, final String ryaInstanceName) {
        requireNonNull(client);
        requireNonNull(ryaInstanceName);
        pcjCollection = client.getDatabase(ryaInstanceName).getCollection(PCJ_COLLECTION_NAME);
    }

    private String getMetadataID(final String pcjName) {
        return pcjName + "_METADATA";
    }

    /**
     * Creates a {@link Document} containing the metadata defining the PCj.
     * @param pcjName - The name of the PCJ. (not null)
     * @param sparql - The sparql query the PCJ will use.
     * @return The document built around the provided metadata.
     * @throws PCJStorageException - Thrown when the sparql query is malformed.
     */
    public Document getMetadataDocument(final String pcjName, final String sparql) throws PCJStorageException {
        requireNonNull(pcjName);
        requireNonNull(sparql);

        final Set<VariableOrder> varOrders;
        try {
            varOrders = pcjVarOrderFactory.makeVarOrders(sparql);
        } catch (final MalformedQueryException e) {
            throw new PCJStorageException("Can not create the PCJ. The SPARQL is malformed.", e);
        }

        return new Document()
                .append(PCJ_ID, getMetadataID(pcjName))
                .append(SPARQL_FIELD, sparql)
                .append(CARDINALITY_FIELD, 0)
                .append(VAR_ORDER_ID, varOrders);

    }

    /**
     * Creates a new PCJ based on the provided metadata.  The initial pcj results will be empty.
     * @param pcjName - The unique name of the PCJ.
     * @param varOrders - The {@link VariableOrder}s.
     * @param sparql - The query the pcj is assigned to.
     * @throws @throws PCJStorageException - Thrown when the sparql query is malformed.
     */
    public void createPcj(final String pcjName, final String sparql) throws PCJStorageException {
        pcjCollection.insertOne(getMetadataDocument(pcjName, sparql));
    }

    /**
     * Creates a new PCJ document and populates it by scanning an instance of
     * Rya for historic matches.
     * <p>
     * If any portion of this operation fails along the way, the partially
     * create PCJ table will be left in Accumulo.
     *
     * @param ryaConn - Connects to the Rya that will be scanned. (not null)
     * @param pcjTableName - The name of the PCJ table that will be created. (not null)
     * @param sparql - The SPARQL query whose results will be loaded into the table. (not null)
     * @throws PCJStorageException The PCJ table could not be create or the
     *     values from Rya were not able to be loaded into it.
     */
    public void createAndPopulatePcj(
            final RepositoryConnection ryaConn,
            final String pcjTableName,
            final String sparql) throws PCJStorageException {
        checkNotNull(pcjTableName);
        checkNotNull(sparql);

        // Create the PCJ document in Mongo.
        createPcj(pcjTableName, sparql);

        // Load historic matches from Rya into the PCJ table.
        populatePcj(pcjTableName, ryaConn);
    }

    /**
     * Gets the {@link PcjMetadata} from a provided PCJ name.
     *
     * @param pcjName - The PCJ to get from MongoDB. (not null)
     * @return - The {@link PcjMetadata} of the Pcj specified.
     * @throws PCJStorageException The PCJ Table does not exist.
     */
    public PcjMetadata getPcjMetadata(final String pcjName) throws PCJStorageException {
        requireNonNull(pcjName);

        // since query by ID, there will only be one.
        final Document result = pcjCollection.find(new Document(PCJ_ID, getMetadataID(pcjName))).first();

        if(result == null) {
            throw new PCJStorageException("The PCJ: " + pcjName + " does not exist.");
        }

        final String sparql = result.getString(SPARQL_FIELD);
        final int cardinality = result.getInteger(CARDINALITY_FIELD, 0);
        final List<List<String>> varOrders= (List<List<String>>) result.get(VAR_ORDER_ID);
        final Set<VariableOrder> varOrder = new HashSet<>();
        for(final List<String> vars : varOrders) {
            varOrder.add(new VariableOrder(vars));
        }
        //MongoDB does not need to use VarOrders
        return new PcjMetadata(sparql, cardinality, varOrder);
    }

    /**
     * Adds binding set results to a specific PCJ.
     * @param pcjName - The PCJ to add the results to.
     * @param results - The binding set results.
     */
    public void addResults(final String pcjName, final Collection<VisibilityBindingSet> results) {
        final List<Document> pcjDocs = new ArrayList<>();
        for (final VisibilityBindingSet vbs : results) {
            // each binding gets it's own doc.
            final Document bindingDoc = new Document(PCJ_NAME, pcjName);
            vbs.forEach(binding -> {
                final RyaType type = RdfToRyaConversions.convertValue(binding.getValue());
                bindingDoc.append(binding.getName(),
                        new Document()
                        .append(BINDING_TYPE, type.getDataType().stringValue())
                        .append(BINDING_VALUE, type.getData())
                        );
            });
            bindingDoc.append(AUTHS_FIELD, vbs.getVisibility());
            pcjDocs.add(bindingDoc);
        }
        pcjCollection.insertMany(pcjDocs);

        // update cardinality in the metadata doc.
        final int appendCardinality = pcjDocs.size();
        final Bson query = new Document(PCJ_ID, getMetadataID(pcjName));
        final Bson update = new Document("$inc", new Document(CARDINALITY_FIELD, appendCardinality));
        pcjCollection.updateOne(query, update);
    }

    /**
     * Purges all results from the PCJ document with the provided name.
     * @param pcjName - The name of the PCJ document to purge. (not null)
     */
    public void purgePcjs(final String pcjName) {
        requireNonNull(pcjName);

        // remove every doc for the pcj, except the metadata
        final Bson filter = new Document(PCJ_NAME, pcjName);
        pcjCollection.deleteMany(filter);

        // reset cardinality
        final Bson query = new Document(PCJ_ID, getMetadataID(pcjName));
        final Bson update = new Document("$set", new Document(CARDINALITY_FIELD, 0));
        pcjCollection.updateOne(query, update);
    }

    /**
     * Scan Rya for results that solve the PCJ's query and store them in the PCJ document.
     * <p>
     * This method assumes the PCJ document has already been created.
     *
     * @param pcjTableName - The name of the PCJ table that will receive the results. (not null)
     * @param ryaConn - A connection to the Rya store that will be queried to find results. (not null)
     * @throws PCJStorageException If results could not be written to the PCJ table,
     *   the PCJ table does not exist, or the query that is being execute
     *   was malformed.
     */
    public void populatePcj(final String pcjTableName, final RepositoryConnection ryaConn) throws PCJStorageException {
        checkNotNull(pcjTableName);
        checkNotNull(ryaConn);

        try {
            // Fetch the query that needs to be executed from the PCJ table.
            final PcjMetadata pcjMetadata = getPcjMetadata(pcjTableName);
            final String sparql = pcjMetadata.getSparql();

            // Query Rya for results to the SPARQL query.
            final TupleQuery query = ryaConn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
            final TupleQueryResult results = query.evaluate();

            // Load batches of 1000 of them at a time into the PCJ table
            final Set<VisibilityBindingSet> batch = new HashSet<>(1000);
            while(results.hasNext()) {
                final VisibilityBindingSet bs = new VisibilityBindingSet(results.next());
                batch.add( bs );
                System.out.println(bs.toString());
                if(batch.size() == 1000) {
                    addResults(pcjTableName, batch);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()) {
                addResults(pcjTableName, batch);
            }

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
            throw new PCJStorageException("Could not populate a PCJ document with Rya results for the pcj named: " + pcjTableName, e);
        }
    }

    /**
     * List the document names of the PCJ index tables that are stored in MongoDB
     * for this instance of Rya.
     *
     * @return A list of pcj document names that hold PCJ index data for the current
     *   instance of Rya.
     */
    public List<String> listPcjDocuments() {
        final List<String> pcjNames = new ArrayList<>();

        //This Bson string reads as:
        //{} - no search criteria: find all
        //{ _id: 1 } - only return the _id, which is the PCJ name.
        final FindIterable<Document> rez = pcjCollection.find((Bson) JSON.parse("{ }, { " + PCJ_ID + ": 1 , _id: 0}"));
        final Iterator<Document> iter = rez.iterator();
        while(iter.hasNext()) {
            pcjNames.add(iter.next().get(PCJ_ID).toString().replace("_METADATA", ""));
        }

        return pcjNames;
    }

    /**
     * Returns all of the results of a PCJ.
     *
     * @param pcjName - The PCJ to get the results for. (not null)
     * @return The authorized PCJ results.
     */
    public CloseableIterator<BindingSet> listResults(final String pcjName) {
        requireNonNull(pcjName);

        // get all results based on pcjName
        return queryForBindings(new Document(PCJ_NAME, pcjName));
    }

    /**
     * Retrieves the stored {@link BindingSet} results for the provided pcjName.
     *
     * @param pcjName - The pcj to retrieve results for.
     * @param authorizations - The authorizations of the user to restrict results.
     * @param bindingset - The collection of {@link BindingSet}s to restrict results.
     * <p>
     * Note: the result restrictions from {@link BindingSet}s are an OR over ANDS in that:
     * <code>
     *  [
     *     bindingset: binding AND binding AND binding,
     *     OR
     *     bindingset: binding AND binding AND binding,
     *     .
     *     .
     *     .
     *     OR
     *     bindingset: binding
     *  ]
     * </code>
     * @return
     */
    public CloseableIterator<BindingSet> getResults(final String pcjName, final Authorizations authorizations, final Collection<BindingSet> bindingset) {
        // empty bindings return all results.
        if (bindingset.size() == 1 && bindingset.iterator().next().size() == 0) {
            return listResults(pcjName);
        }

        final Document query = new Document(PCJ_NAME, pcjName);
        final Document bindingSetDoc = new Document();
        final List<Document> bindingSetList = new ArrayList<>();
        bindingset.forEach(bindingSet -> {
            final Document bindingDoc = new Document();
            final List<Document> bindings = new ArrayList<>();
            bindingSet.forEach(binding -> {
                final RyaType type = RdfToRyaConversions.convertValue(binding.getValue());
                final Document typeDoc = new Document()
                        .append(BINDING_TYPE, type.getDataType().stringValue())
                        .append(BINDING_VALUE, type.getData());
                final Document bind = new Document(binding.getName(), typeDoc);
                bindings.add(bind);
            });
            bindingDoc.append("$and", bindings);
            bindingSetList.add(bindingDoc);
        });
        bindingSetDoc.append("$or", bindingSetList);
        return queryForBindings(query);
    }

    private CloseableIterator<BindingSet> queryForBindings(final Document query) {
        final FindIterable<Document> rez = pcjCollection.find(query);
        final Iterator<Document> resultsIter = rez.iterator();
        return new CloseableIterator<BindingSet>() {
            @Override
            public boolean hasNext() {
                return resultsIter.hasNext();
            }

            @Override
            public BindingSet next() {
                final Document bs = resultsIter.next();
                final MapBindingSet binding = new MapBindingSet();
                for (final String key : bs.keySet()) {
                    if (key.equals(AUTHS_FIELD)) {
                        // has auths, is a visibility binding set.
                    } else if (!key.equals("_id") && !key.equals(PCJ_NAME)) {
                        // is the binding value.
                        final Document typeDoc = (Document) bs.get(key);
                        final URI dataType = new URIImpl(typeDoc.getString(BINDING_TYPE));
                        final RyaType type = new RyaType(dataType, typeDoc.getString(BINDING_VALUE));
                        final Value value = RyaToRdfConversions.convertValue(type);
                        binding.addBinding(key, value);
                    }
                }
                return binding;
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
     * @param pcjName - The identifier for the PCJ to remove.
     */
    public void dropPcjTable(final String pcjName) {
        purgePcjs(pcjName);
        pcjCollection.deleteOne(new Document(PCJ_ID, getMetadataID(pcjName)));
    }
}
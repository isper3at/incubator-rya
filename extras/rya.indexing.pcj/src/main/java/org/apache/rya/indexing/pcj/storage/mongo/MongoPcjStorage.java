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

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.indexing.pcj.storage.AbstractPcjStorage;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A mongo backed implementation of {@link PrecomputedJoinStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class MongoPcjStorage extends AbstractPcjStorage {
    private static final String PCJ_COLLECTION_NAME = "pcjs";

    private final MongoClient client;
    private final MongoPcjDocuments pcjDocs;
    private final Authorizations userAuths;

    /**
     * Constructs an instance of {@link MongoPcjStorage}.
     *
     * @param client - The {@link MongoClient} that will be used to connect to Mongodb. (not null)
     * @param ryaInstanceName - The name of the RYA instance that will be accessed. (not null)
     * @param userAuths - The Authorizations of the current user.
     */
    public MongoPcjStorage(final MongoClient client, final String ryaInstanceName, final Authorizations userAuths) {
        super(new MongoRyaInstanceDetailsRepository(requireNonNull(client), requireNonNull(ryaInstanceName)),
                ryaInstanceName);
        this.client = client;
        pcjDocs = new MongoPcjDocuments(client, ryaInstanceName);
        this.userAuths = userAuths;
    }

    @Override
    public String createPcj(final String sparql) throws PCJStorageException {
        requireNonNull(sparql);

        // Create the variable orders that will be used within Accumulo to store
        // the PCJ.
        final Set<VariableOrder> varOrders;
        try {
            varOrders = pcjVarOrderFactory.makeVarOrders(sparql);
        } catch (final MalformedQueryException e) {
            throw new PCJStorageException("Can not create the PCJ. The SPARQL is malformed.", e);
        }

        // Update the Rya Details for this instance to include the new PCJ
        // table.
        final String pcjId = pcjIdFactory.nextId();
        super.addPcj(pcjId);

        // Create the objectID of the document to house the PCJ results.
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjDocs.createPcj(pcjTableName, varOrders, sparql);

        // Add access to the PCJ table to all users who are authorized for this
        // instance of Rya.
        return pcjId;
    }


    @Override
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        return pcjDocs.getPcjMetadata(pcjTableName);
    }

    @Override
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results)
            throws PCJStorageException {
        requireNonNull(pcjId);
        requireNonNull(results);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjDocs.addResults(pcjTableName, results);
    }


    @Override
    public CloseableIterator<BindingSet> listResults(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        // Scan the PCJ table.
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        return pcjDocs.listResults(pcjTableName, userAuths);
    }

    @Override
    public void purge(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjDocs.purgePcjTable(pcjTableName);
    }

    @Override
    public void dropPcj(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);

        // Update the Rya Details for this instance to no longer include the
        // PCJ.
        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(originalDetails -> {
                // Drop the PCJ's metadata from the instance's metadata.
                final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                mutated.getPCJIndexDetails().removePCJDetails(pcjId);
                return mutated.build();
            });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not drop an existing PCJ for Rya instance '%s' "
                    + "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }

        // Delete the table that hold's the PCJ's results.
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjDocs.dropPcjTable(pcjTableName);
    }

    @Override
    public void close() throws PCJStorageException {
        // shutdown hook.
        client.close();
    }
}
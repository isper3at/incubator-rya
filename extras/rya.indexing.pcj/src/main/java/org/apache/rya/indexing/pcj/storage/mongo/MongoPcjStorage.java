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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
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
 * An Accumulo backed implementation of {@link PrecomputedJoinStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class MongoPcjStorage extends AbstractPcjStorage {
    private static final String PCJ_COLLECTION_NAME = "pcjs";

    private final MongoClient client;
    private final MongoPcjDocuments pcjTables;
    /**
     * Constructs an instance of {@link MongoPcjStorage}.
     *
     * @param client - The {@link MongoClient} that will be used to connect to Mongodb. (not null)
     * @param ryaInstanceName - The name of the RYA instance that will be accessed. (not null)
     */
    public MongoPcjStorage(final MongoClient client, final String ryaInstanceName) {
        super(new MongoRyaInstanceDetailsRepository(requireNonNull(client), requireNonNull(ryaInstanceName)),
                ryaInstanceName);
        this.client = client;
        pcjTables = new MongoPcjDocuments(client, ryaInstanceName);
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
        pcjTables.createPcjTable(pcjTableName, varOrders, sparql);

        // Add access to the PCJ table to all users who are authorized for this
        // instance of Rya.
        try {
            for (final String user : ryaDetailsRepo.getRyaInstanceDetails().getUsers()) {
                TABLE_PERMISSIONS.grantAllPermissions(user, pcjTableName, accumuloConn);
            }
        } catch (final RyaDetailsRepositoryException | AccumuloException | AccumuloSecurityException e) {
            throw new PCJStorageException(String.format("Could not grant authorized users access to the "
                    + "new PCJ index table '%s' for Rya instance '%s' because of a problem while granting "
                    + "the permissions.", pcjTableName, ryaInstanceName), e);
        }

        return pcjId;
    }

    @Override
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results)
            throws PCJStorageException {
        // TODO Auto-generated method stub

    }

    @Override
    public CloseableIterator<BindingSet> listResults(final String pcjId) throws PCJStorageException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void purge(final String pcjId) throws PCJStorageException {
        // TODO Auto-generated method stub

    }

    @Override
    public void dropPcj(final String pcjId) throws PCJStorageException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws PCJStorageException {
        // TODO Auto-generated method stub

    }

}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client.mongo;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.LoadStatements;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link LoadStatements} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoLoadStatements implements LoadStatements {

    private final MongoConnectionDetails connectionDetails;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoLoadStatements(final MongoConnectionDetails connectionDetails, final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public void loadStatements(final String ryaInstanceName, final Iterable<? extends Statement> statements) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(statements);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        Sail sail = null;
        SailRepository sailRepo = null;
        SailRepositoryConnection sailRepoConn = null;

        // Get a Sail object that is connected to the Rya instance.
        final MongoDBRdfConfiguration ryaConf = connectionDetails.build(ryaInstanceName);
        try {
            sail = RyaSailFactory.getInstance(ryaConf);
        } catch (SailException | RyaDAOException | InferenceEngineException | AccumuloException | AccumuloSecurityException e) {
            throw new RyaClientException("Could not load statements into Rya because of a problem while creating the Sail object.", e);
        }

        // Load the file.
        sailRepo = new SailRepository(sail);
        try {
            sailRepoConn = sailRepo.getConnection();
            sailRepoConn.add(statements);
        } catch (final RepositoryException e) {
            throw new RyaClientException("While getting a connection and adding statements.", e);
        }
    }
}
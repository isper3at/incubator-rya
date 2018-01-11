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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import org.apache.rya.api.client.AddEntityType;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.indexing.entity.storage.mongo.MongoTypeStorage;

import com.mongodb.MongoClient;

/**
 * A Mongo implementation of {@link AddEntityType}.
 */
public class MongoAddEntityType implements AddEntityType {
    private final InstanceExists instanceExists;
    private final MongoClient mongoClient;

    /**
     * Constructs an instance of {@link MongoAddEntityType}.
     *
     * @param mongoClient - The {@link MongoClient} to use to delete a PCJ. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoAddEntityType(
            final MongoClient mongoClient,
            final MongoInstanceExists instanceExists) {
        this.mongoClient = requireNonNull(mongoClient);
        this.instanceExists = requireNonNull(instanceExists);
    }

	@Override
	public void addEntityType(final String ryaInstanceName, final EntityTypeConfiguration typeConfiguration) throws InstanceDoesNotExistException, RyaClientException {
		requireNonNull(ryaInstanceName);
        checkState(instanceExists.exists(ryaInstanceName), "The instance: " + ryaInstanceName + " does not exist.");

        final MongoTypeStorage typeStore = new MongoTypeStorage(mongoClient, ryaInstanceName);
        final Type type = new Type(typeConfiguration.getId(), typeConfiguration.getProperties());
        try {
            typeStore.create(type);
        } catch (final TypeStorageException e) {
            throw new RyaClientException("Could not add Type: " + type.getId().toString(), e);
        }
//
//        final RyaURI id = promptID();
//
//        final ImmutableSet.Builder<RyaURI> propBuilder = ImmutableSet.builder();
//        RyaURI property = null;
//        do {
//            property = promptProperty();
//            if(property != null) {
//                propBuilder.add(property);
//            }
//        } while(property != null);

	}
}
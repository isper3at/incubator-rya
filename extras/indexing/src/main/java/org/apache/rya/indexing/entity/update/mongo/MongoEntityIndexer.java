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
package org.apache.rya.indexing.entity.update.mongo;

import static com.google.common.base.Preconditions.checkState;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoTypeStorage;
import org.apache.rya.indexing.entity.update.BaseEntityIndexer;
import org.apache.rya.indexing.entity.update.EntityIndexer;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Mongo DB implementation of {@link EntityIndexer}.
 */
@DefaultAnnotation(NonNull.class)
public class MongoEntityIndexer extends BaseEntityIndexer {
    private MongoClient client;

    @Override
    public EntityStorage getEntityStorage(final Configuration conf) throws EntityStorageException {
        checkState(conf instanceof StatefulMongoDBRdfConfiguration,
                "The provided Configuration must be of type StatefulMongoDBRdfConfiguration, but was "  + conf.getClass().getName());

        final StatefulMongoDBRdfConfiguration mongoConf = (StatefulMongoDBRdfConfiguration) conf;
        if (client == null) {
            client = mongoConf.getMongoClient();
        }
        final String ryaInstanceName = mongoConf.getMongoDBName();
        return new MongoEntityStorage(client, ryaInstanceName);
    }

    @Override
    public TypeStorage getTypeStorage(final Configuration conf) {
        checkState(conf instanceof StatefulMongoDBRdfConfiguration,
                "The provided Configuration must be of type StatefulMongoDBRdfConfiguration, but was "  + conf.getClass().getName());

        final StatefulMongoDBRdfConfiguration mongoConf = (StatefulMongoDBRdfConfiguration) conf;
        if (client == null) {
            client = mongoConf.getMongoClient();
        }
        final String ryaInstanceName = mongoConf.getMongoDBName();
        return new MongoTypeStorage(client, ryaInstanceName);
    }


    @Override
    public void init() {
        //nothing to init.
    }

    @Override
    public void setClient(final MongoClient client) {
        this.client = client;
    }
}
/**
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
package org.apache.rya.streams.mongo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;

import com.mongodb.MongoClient;
import com.mongodb.client.model.CreateCollectionOptions;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A capped collection on MongoDB to keep a history of CREATE, DELETE, START,
 * and STOP query commands on Rya Streams Kafka.
 */
@DefaultAnnotation(NonNull.class)
public class MongoKafkaOpLogCollection {
    private static final String COLLECTION_NAME = "kafka-history";

    private final MongoClient client;
    private final String ryaInstance;

    /**
     * Creates a new {@link MongoKafkaOpLogCollection}.
     *
     * @param client - The {@link MongoClient} to use to connect to MongoDB. (not null)
     * @param ryaInstance - The Rya instance to connect to. (not null)
     * @param maxDocuments - The maximum number of documents to allow in this collection. (not null)
     */
    public MongoKafkaOpLogCollection(final MongoClient client, final String ryaInstance,
            final int maxDocuments) {
        this.client = requireNonNull(client);
        this.ryaInstance = requireNonNull(ryaInstance);

        // check to see if the collection exists or not, the collection needs to be capped.
        if (client.getDatabase(ryaInstance).listCollectionNames().into(new ArrayList<>()).contains(COLLECTION_NAME)) {
            client.getDatabase(ryaInstance).createCollection(COLLECTION_NAME,
                    new CreateCollectionOptions()
                        .capped(true)
                        .sizeInBytes(4096)
                        .maxDocuments(maxDocuments));
        }
    }
}

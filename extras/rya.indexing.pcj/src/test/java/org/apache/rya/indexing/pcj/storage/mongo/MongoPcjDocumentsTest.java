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

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.mongodb.MockMongoFactory;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;

public class MongoPcjDocumentsTest {
    private static final String INSTANCE = "TEST";
    private MongoClient client;
    private MongoPcjDocuments docStore;

    @Before
    public void setup() throws Exception {
        client = MockMongoFactory.newFactory().newMongoClient();
        docStore = new MongoPcjDocuments(client, INSTANCE);
    }

    @Test
    public void serialize_createPcjDocument() throws Exception {
        final String sparql = "some sparql";
        final String pcjName = "somePcj";
        final Set<VariableOrder> varOrders = new HashSet<>();
        varOrders.add(new VariableOrder("A", "B", "C"));
        varOrders.add(new VariableOrder("B", "C", "A"));
        varOrders.add(new VariableOrder("C", "A", "B"));
        docStore.createPcj(pcjName, varOrders, sparql);

        final Iterable<Document> rez = client.getDatabase(INSTANCE).getCollection("pcjs").find();
        final Iterator<Document> results = rez.iterator();

        final Document expected = new Document()
            .append("_id", "somePcj")
            .append("sparql", "some sparql")
            .append("var_orders", "");


        int count = 0;
        while (results.hasNext()) {
            count++;
            assertEquals(expected, results.next());
        }
        assertEquals(1, count);
    }
}

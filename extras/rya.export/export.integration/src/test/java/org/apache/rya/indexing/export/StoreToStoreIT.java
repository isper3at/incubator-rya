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
package org.apache.rya.indexing.export;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.parent.AccumuloParentMetadataRepository;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.mongodb.MongoClient;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.mongodb.MongoDBRyaDAO;

@RunWith(Parameterized.class)
public class StoreToStoreIT extends ITBase {
    private static final String RYA_INSTANCE = "ryaInstance";
    private final RyaStatementStore parentStore;
    private final RyaStatementStore childStore;

    private final ParentMetadataRepository parentMetadata;
    private final ParentMetadataRepository childMetadata;

    @Parameterized.Parameters
    public static Collection<Object[]> instancesToTest() throws Exception {
        final MongoClient mongoClient1 = ITBase.getNewMongoResources(RYA_INSTANCE);
        final MongoClient mongoClient2 = ITBase.getNewMongoResources(RYA_INSTANCE);
        final MongoDBRyaDAO mongodao1 = new MongoDBRyaDAO(ITBase.getConf(mongoClient1), mongoClient1);
        final MongoDBRyaDAO mongodao2 = new MongoDBRyaDAO(ITBase.getConf(mongoClient2), mongoClient2);
        final MongoRyaStatementStore mongoStore1 = new MongoRyaStatementStore(mongoClient1, RYA_INSTANCE, mongodao1);
        final MongoRyaStatementStore mongoStore2 = new MongoRyaStatementStore(mongoClient2, RYA_INSTANCE, mongodao2);
        final MongoParentMetadataRepository mongoMetadata1 = new MongoParentMetadataRepository(mongoClient1, RYA_INSTANCE);
        final MongoParentMetadataRepository mongoMetadata2 = new MongoParentMetadataRepository(mongoClient2, RYA_INSTANCE);

        final InstanceType type = InstanceType.MOCK;
        final String tablePrefix = "accumuloTest";
        final String auths = "U";
        final AccumuloInstanceDriver aDriver1 = new AccumuloInstanceDriver(RYA_INSTANCE, type, true, false, true, "TEST", PASSWORD, RYA_INSTANCE, tablePrefix, auths, "");
        aDriver1.setUp();
        final AccumuloRyaStatementStore accumuloStore1 = new AccumuloRyaStatementStore(aDriver1.getDao(), tablePrefix, RYA_INSTANCE);
        final AccumuloParentMetadataRepository accumuloMetadata1 = new AccumuloParentMetadataRepository(aDriver1.getDao());

        final AccumuloInstanceDriver aDriver2 = new AccumuloInstanceDriver(RYA_INSTANCE, type, true, false, true, "TEST1", PASSWORD, RYA_INSTANCE, tablePrefix, auths, "");
        aDriver2.setUp();
        final AccumuloRyaStatementStore accumuloStore2 = new AccumuloRyaStatementStore(aDriver2.getDao(), tablePrefix, RYA_INSTANCE);
        final AccumuloParentMetadataRepository accumuloMetadata2 = new AccumuloParentMetadataRepository(aDriver2.getDao());

        final Collection<Object[]> stores = new ArrayList<>();
        //stores.add(new Object[]{mongoStore1, mongoMetadata1, mongoStore2, mongoMetadata2});
        //stores.add(new Object[]{mongoStore1, mongoMetadata1, accumuloStore2, accumuloMetadata2});
        stores.add(new Object[]{accumuloStore1, accumuloMetadata1, mongoStore2, mongoMetadata2});
        //stores.add(new Object[]{accumuloStore1, accumuloMetadata1, accumuloStore2, accumuloMetadata2});
        return stores;
    }

    public StoreToStoreIT(final RyaStatementStore parentStore,
            final ParentMetadataRepository parentMetadata,
            final RyaStatementStore childStore,
            final ParentMetadataRepository childMetadata) {
        this.parentStore = parentStore;
        this.parentMetadata = parentMetadata;

        this.childStore = childStore;
        this.childMetadata = childMetadata;
    }

    //@Test
    public void cloneTest() throws AddStatementException, FetchStatementException, ParentMetadataDoesNotExistException {
        final Date currentTime = new Date(0L);
        loadMockStatements(parentStore, 50);
        System.out.println(count(parentStore));

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            parentMetadata, childMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        merger.runJob();
        assertEquals(50, count(childStore));
    }

    //@Test
    public void no_statementsTest() throws AddStatementException, FetchStatementException {
        loadMockStatements(parentStore, 50);
        Date currentTime = new Date();
        //ensure current time is later
        currentTime = new Date(currentTime.getTime() + 10000L);

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        merger.runJob();
        assertEquals(0, count(childStore));
    }

    //@Test
    public void childToParent_ChildAddTest() throws AddStatementException, FetchStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            parentMetadata, childMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        merger.runJob();

        //add a few statements to child
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://51");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://52");
        childStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        otherMerger.runJob();
        assertEquals(52, count(parentStore));
    }

    //@Test
    public void childToParent_ChildReAddsDeletedStatementTest() throws AddStatementException, RemoveStatementException, FetchStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        merger.runJob();

        //remove a statement from the parent
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://1");
        parentStore.removeStatement(stmnt1);
        assertEquals(49, count(parentStore));

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        otherMerger.runJob();
        //merging will have added the statement back
        assertEquals(50, count(parentStore));
    }

    //@Test
    public void childToParent_BothAddTest() throws AddStatementException, FetchStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        merger.runJob();

        //add a statement to each store
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://51");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://52");
        parentStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);


        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new VisibilityStatementMerger(), currentTime, RYA_INSTANCE, 0L);
        otherMerger.runJob();
        //both should still be there
        assertEquals(52, count(parentStore));
    }

    private void loadMockStatements(final RyaStatementStore store, final int count) throws AddStatementException {
        for(int ii = 0; ii < count; ii++) {
            final RyaStatement statement = makeRyaStatement("http://subject", "http://predicate", "http://"+ii);
            statement.setTimestamp(new Date().getTime());
            parentStore.addStatement(statement);
        }
    }

    private int count(final RyaStatementStore store) throws FetchStatementException {
        final Iterator<RyaStatement> statements = store.fetchStatements();
        int count = 0;
        while(statements.hasNext()) {
            statements.next();
            count++;
        }
        return count;
    }
}

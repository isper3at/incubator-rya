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
package org.apache.rya.indexing.pcj.fluo.api;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.junit.Test;

import com.google.common.io.Files;

import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.mini.MiniFluo;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;

/**
 * Tests the methods of {@link CountStatements}.
 */
public class CountStatementsIT extends ITBase {

    /**
     * Overriden so that no Observers will be started. This ensures whatever
     * statements are inserted as part of the test will not be consumed.
     *
     * @return A Mini Fluo cluster.
     */
    @Override
    protected MiniFluo startMiniFluo() {
        final File miniDataDir = Files.createTempDir();

        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverConfiguration> observers = new ArrayList<>();

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setApplicationName("IntegrationTests");
        config.setMiniDataDir(miniDataDir.getAbsolutePath());
        config.addObservers(observers);

        final MiniFluo miniFluo = FluoFactory.newMiniFluo(config);
        return miniFluo;
    }

    @Test
    public void test() {
        // Insert some Triples into the Fluo app.
        final Map<RyaStatement, String> triples = new HashMap<>();
        addStatementEmptyVisibilityEntry(triples, makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"));
        addStatementEmptyVisibilityEntry(triples, makeRyaStatement("http://Bob", "http://talksTo", "http://Alice"));
        addStatementEmptyVisibilityEntry(triples, makeRyaStatement("http://Charlie", "http://talksTo", "http://Bob"));
        addStatementEmptyVisibilityEntry(triples, makeRyaStatement("http://David", "http://talksTo", "http://Bob"));
        addStatementEmptyVisibilityEntry(triples, makeRyaStatement("http://Eve", "http://talksTo", "http://Bob"));

        new InsertTriples().insert(fluoClient, triples);

        // Load some statements into the Fluo app.
        final BigInteger count = new CountStatements().countStatements(fluoClient);

        // Ensure the count matches the expected values.
        assertEquals(BigInteger.valueOf(5), count);
    }
}
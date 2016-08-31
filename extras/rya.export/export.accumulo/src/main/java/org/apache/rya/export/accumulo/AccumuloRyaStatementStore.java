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
package org.apache.rya.export.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.accumulo.util.AccumuloRyaUtils;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.store.RyaStatementStore;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Allows specific CRUD operations an Accumulo {@link RyaStatement} storage
 * system.
 * <p>
 * The operations specifically:
 * <li>fetch all rya statements in the store</li>
 * <li>add a rya statement to the store</li>
 * <li>remove a rya statement from the store</li>
 * <li>update an existing rya statement with a new one</li>
 *
 * One would use this {@link AccumuloRyaStatementStore} when they have an
 * Accumulo database that is used when merging in data or exporting data.
 */
public class AccumuloRyaStatementStore implements RyaStatementStore {
    private static final Logger log = Logger.getLogger(AccumuloRyaStatementStore.class);

    private AccumuloRyaDAO accumuloRyaDAO;
    private final Configuration config;
    private AccumuloRdfConfiguration accumuloRdfConfiguration;
    private RyaTripleContext ryaTripleContext;
    private String tablePrefix;
    private final AtomicBoolean isInitialized = new AtomicBoolean();

    /**
     * Creates a new instance of {@link AccumuloRyaStatementStore}.
     * @param config the {@link Configuration}.
     */
    public AccumuloRyaStatementStore(final Configuration config) {
        this.config = checkNotNull(config);
    }

    /**
     * @return the {@link Configuration}.
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * @return the {@link AccumuloRdfConfiguration}.
     */
    public RdfCloudTripleStoreConfiguration getRdfCloudTripleStoreConfiguration() {
        return accumuloRdfConfiguration;
    }

    /**
     * @return the {@link RyaTripleContext}.
     */
    @Override
    public RyaTripleContext getRyaTripleContext() {
        return ryaTripleContext;
    }

    @Override
    public void init() throws MergerException {
        if (isInitialized.get()) {
            throw new MergerException("AccumuloRyaStatementStore already initialized");
        } else {
            final String instance = config.get(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
            final String userName = config.get(ConfigUtils.CLOUDBASE_USER, "root");
            final String pwd = config.get(ConfigUtils.CLOUDBASE_PASSWORD, "password");
            final InstanceType instanceType = InstanceType.fromName(config.get(AccumuloExportConstants.ACCUMULO_INSTANCE_TYPE_PROP, InstanceType.DISTRIBUTION.toString()));
            tablePrefix = config.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
            if (tablePrefix != null) {
                RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
            }
            final String auth = config.get(ConfigUtils.CLOUDBASE_AUTHS);

            accumuloRdfConfiguration = new AccumuloRdfConfiguration(config);
            accumuloRdfConfiguration.setTablePrefix(tablePrefix);
            ryaTripleContext = RyaTripleContext.getInstance(accumuloRdfConfiguration);

            final AccumuloInstanceDriver accumuloInstanceDriver = new AccumuloInstanceDriver(AccumuloRyaStatementStore.class.getSimpleName(), instanceType, true, false, true, userName, pwd, instance, tablePrefix, auth);
            try {
                accumuloInstanceDriver.setUp();
            } catch (final Exception e) {
                throw new MergerException(e);
            }
            accumuloRyaDAO = accumuloInstanceDriver.getDao();
            isInitialized.set(true);
        }
    }

    @Override
    public boolean isInitialized() {
        return isInitialized.get();
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() throws MergerException {
        try {
            final RyaTripleContext ryaTripleContext = RyaTripleContext.getInstance(accumuloRdfConfiguration);

            Scanner scanner = null;
            try {
                scanner = AccumuloRyaUtils.getScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, config);
            } catch (final IOException e) {
                log.error("Unable to get scanner to fetch Rya Statements", e);
            }
            // Convert Entry iterator to RyaStatement iterator
            final Iterator<Entry<Key, Value>> entryIter = scanner.iterator();
            final Iterator<RyaStatement> ryaStatementIter = Iterators.transform(entryIter, new Function<Entry<Key, Value>, RyaStatement>() {
               @Override
               public RyaStatement apply(final Entry<Key, Value> entry) {
                   final Key key = entry.getKey();
                   final Value value = entry.getValue();
                   RyaStatement ryaStatement = null;
                   try {
                       ryaStatement = AccumuloRyaUtils.createRyaStatement(key, value, ryaTripleContext);
                   } catch (final TripleRowResolverException e) {
                       log.error("Unable to convert the key/value pair into a Rya Statement", e);
                   }
                   return ryaStatement;
               }
            });
            return ryaStatementIter;
        } catch (final Exception e) {
            throw new MergerException("Failed to fetch statements.", e);
        }
    }

    @Override
    public void addStatement(final RyaStatement statement) throws MergerException {
        try {
            accumuloRyaDAO.add(statement);
        } catch (final RyaDAOException e) {
            throw new MergerException("Unable to add the Rya Statement", e);
        }
    }

    @Override
    public void removeStatement(final RyaStatement statement) throws MergerException {
        try {
            accumuloRyaDAO.delete(statement, accumuloRdfConfiguration);
        } catch (final RyaDAOException e) {
            throw new MergerException("Unable to delete the Rya Statement", e);
        }
    }

    @Override
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws MergerException {
        removeStatement(original);
        addStatement(update);
    }

    @Override
    public boolean containsStatement(final RyaStatement ryaStatement) throws MergerException {
        CloseableIteration<RyaStatement, RyaDAOException> iter = null;
        try {
            iter = findStatement(ryaStatement);
            return iter.hasNext();
        } catch (final RyaDAOException e) {
            throw new MergerException("Encountered an error while querying for statement.", e);
        } finally {
            try {
                if (iter != null) {
                    iter.close();
                }
            } catch (final RyaDAOException e) {
                throw new MergerException("Error closing query iterator.", e);
            }
        }
    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> findStatement(final RyaStatement ryaStatement) throws MergerException {
        CloseableIteration<RyaStatement, RyaDAOException> iter = null;
        try {
            iter = accumuloRyaDAO.getQueryEngine().query(ryaStatement, accumuloRdfConfiguration);
            return iter;
        } catch (final RyaDAOException e) {
            throw new MergerException("Encountered an error while querying for statement.", e);
        }
    }
}
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
package org.apache.rya.export.client;

import static org.apache.rya.export.DBType.ACCUMULO;
import static org.apache.rya.export.client.conf.MergeConfigurationCLI.DATE_FORMAT;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.parent.AccumuloParentMetadataRepository;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigHadoopAdapter;
import org.apache.rya.export.client.conf.MergeConfigurationCLI;
import org.apache.rya.export.client.conf.TimeUtils;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.apache.rya.export.mongo.time.TimeMongoRyaStatementStore;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.mongodb.MongoClient;

import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.mongodb.MongoDBRyaDAO;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;

/**
 * Drives the MergeTool.
 */
public class MergeDriverClient {
    private static final Logger LOG = Logger.getLogger(MergeDriverClient.class);
    private static MergeConfiguration configuration;

    public static void main(final String [] args) throws ParseException,
        MergeConfigurationException, UnknownHostException, MergerException,
        java.text.ParseException, SailException, AccumuloException,
        AccumuloSecurityException, InferenceEngineException, RepositoryException,
        MalformedQueryException, UpdateExecutionException {

        final String log4jConfiguration = System.getProperties().getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jConfiguration)) {
            final String parsedConfiguration = StringUtils.removeStart(log4jConfiguration, "file:");
            final File configFile = new File(parsedConfiguration);
            if (configFile.exists()) {
                DOMConfigurator.configure(parsedConfiguration);
            } else {
                BasicConfigurator.configure();
            }
        }

        final MergeConfigurationCLI config = new MergeConfigurationCLI(args);
        try {
            configuration = config.createConfiguration();
        } catch (final MergeConfigurationException e) {
            LOG.error("Configuration failed.", e);
        }

        final Date startTime = DATE_FORMAT.parse(configuration.getToolStartTime());
        RyaStatementStore parentStore = null;
        ParentMetadataRepository parentMetadataRepo = null;

        RyaStatementStore childStore = null;
        ParentMetadataRepository childMetadataRepo = null;

        //This might not be used in query based cloning/merging
        final boolean useTimeSync = configuration.getUseNtpServer();
        Long timeOffset = 0L;
        if (useTimeSync) {
            final String childTomcatHost = configuration.getChildTomcatUrl();
            final String ntpHostname = configuration.getNtpServerHost();
            LOG.info("Getting NTP server time offset.");
            try {
                timeOffset = TimeUtils.getNtpServerAndMachineTimeDifference(ntpHostname, childTomcatHost);
            } catch (final IOException e) {
                LOG.error("Unable to get the time offset between " + childTomcatHost + " and the NTP Server " + ntpHostname + ".", e);
            }
        }

        try {
            if(configuration.getParentDBType() == ACCUMULO) {
                final AccumuloMergeConfiguration accumuloConf = (AccumuloMergeConfiguration) configuration;
                final AccumuloInstanceDriver aInstance = new AccumuloInstanceDriver(
                        accumuloConf.getParentRyaInstanceName(), accumuloConf.getParentInstanceType(),
                        true, false, true, accumuloConf.getParentUsername(), accumuloConf.getParentPassword(),
                        accumuloConf.getParentRyaInstanceName(), accumuloConf.getParentTablePrefix(),
                        accumuloConf.getParentAuths(), accumuloConf.getParentZookeepers());
                try {
                    aInstance.setUp();
                } catch (final Exception e) {
                    throw new MergerException(e);
                }
                parentStore = new AccumuloRyaStatementStore(aInstance.getDao(), aInstance.getTablePrefix(), aInstance.getInstanceName());
                final AccumuloRyaStatementStore aStore = (AccumuloRyaStatementStore) parentStore;
                aStore.addIterator(getAccumuloTimestampIterator(startTime));
                parentMetadataRepo = new AccumuloParentMetadataRepository(aInstance.getDao());
            } else {
                //Mongo
                final MongoClient client = new MongoClient(configuration.getParentHostname(), configuration.getParentPort());
                final MongoDBRyaDAO dao = new MongoDBRyaDAO(new MongoDBRdfConfiguration(MergeConfigHadoopAdapter.getMongoConfiguration(configuration)));
                parentMetadataRepo = new MongoParentMetadataRepository(client, configuration.getParentRyaInstanceName());
                final MongoRyaStatementStore mStore = new MongoRyaStatementStore(client, configuration.getParentRyaInstanceName(), dao);
                parentStore = new TimeMongoRyaStatementStore(mStore, startTime, configuration.getParentRyaInstanceName());
            }

            if(configuration.getChildDBType() == ACCUMULO) {
                final AccumuloMergeConfiguration accumuloConf = (AccumuloMergeConfiguration) configuration;
                final AccumuloInstanceDriver aInstance = new AccumuloInstanceDriver(
                        accumuloConf.getChildRyaInstanceName(), accumuloConf.getChildInstanceType(),
                        true, false, false, accumuloConf.getChildUsername(), accumuloConf.getChildPassword(),
                        accumuloConf.getChildRyaInstanceName(), accumuloConf.getChildTablePrefix(),
                        accumuloConf.getChildAuths(), accumuloConf.getChildZookeepers());
                try {
                    aInstance.setUp();
                } catch (final Exception e) {
                    throw new MergerException(e);
                }
                childStore = new AccumuloRyaStatementStore(aInstance.getDao(), aInstance.getTablePrefix(), aInstance.getInstanceName());
                final AccumuloRyaStatementStore aStore = (AccumuloRyaStatementStore) childStore;
                aStore.addIterator(getAccumuloTimestampIterator(startTime));

                parentMetadataRepo = new AccumuloParentMetadataRepository(aInstance.getDao());
            } else {
                //Mongo
                final MongoClient client = new MongoClient(configuration.getChildHostname(), configuration.getChildPort());
                final MongoDBRyaDAO dao = new MongoDBRyaDAO(new MongoDBRdfConfiguration(MergeConfigHadoopAdapter.getMongoConfiguration(configuration)), client);
                childMetadataRepo = new MongoParentMetadataRepository(client, configuration.getChildRyaInstanceName());
                childStore = new MongoRyaStatementStore(client, configuration.getChildRyaInstanceName(), dao);
            }

            LOG.info("Starting Merge Tool");
            if(configuration.getParentDBType() == ACCUMULO && configuration.getChildDBType() == ACCUMULO) {
                final AccumuloRyaStatementStore childAStore = (AccumuloRyaStatementStore) childStore;
                final AccumuloRyaStatementStore parentAStore = (AccumuloRyaStatementStore) parentStore;

                //do map reduce merging.
                //TODO: Run Merger
            } else {
                final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                    parentMetadataRepo, childMetadataRepo, new VisibilityStatementMerger(),
                    startTime, configuration.getParentRyaInstanceName(), timeOffset);
                merger.runJob();
            }
            LOG.info("Finished running Merge Tool");
        } catch (final NumberFormatException | RyaDAOException e) {
            LOG.error("Failed to run clone/merge tool.", e);
        }

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                LOG.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });
        System.exit(1);
    }

    private static IteratorSetting getAccumuloTimestampIterator(final Date startTime) {
        final IteratorSetting setting = new IteratorSetting(1, "startTimeIterator", TimestampFilter.class);
        TimestampFilter.setStart(setting, startTime.getTime(), true);
        TimestampFilter.setEnd(setting, Long.MAX_VALUE, true);
        return setting;
    }
}

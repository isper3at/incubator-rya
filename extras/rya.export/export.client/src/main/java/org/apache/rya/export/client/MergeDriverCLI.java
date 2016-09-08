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

import java.io.File;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigurationCLI;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;

import com.mongodb.MongoClient;

import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.mongodb.MongoDBRyaDAO;

/**
 * Drives the MergeTool.
 */
public class MergeDriverCLI {
    private static final Logger LOG = Logger.getLogger(MergeDriverCLI.class);
    private static MergeConfiguration configuration;

    public static void main(final String [] args) throws ParseException, MergeConfigurationException, UnknownHostException {
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
        final Configuration parentHadoopConfig = config.getParentHadoopConfig();
        final Configuration childHadoopConfig = config.getChildHadoopConfig();
        try {
            configuration = config.createConfiguration();
        } catch (final MergeConfigurationException e) {
            LOG.error("Configuration failed.", e);
        }

        final Date startTime = config.getRyaStatementMergeTime();
        RyaStatementStore parentStore = null;
        ParentMetadataRepository parentMetadataRepo = null;

        RyaStatementStore childStore = null;
        ParentMetadataRepository childMetadataRepo = null;

        final boolean useTimeSync = false;//configuration.getUseNtpServer();
        if (useTimeSync) {
            /*final String tomcatUrl = configuration.getChildTomcatUrl();
            final String ntpServerHost = configuration.getNtpServerHost();
            Long timeOffset = null;
            try {
                LOG.info("Comparing child machine's time to NTP server time...");
                timeOffset = TimeUtils.getNtpServerAndMachineTimeDifference(ntpServerHost, tomcatUrl);
            } catch (IOException | java.text.ParseException e) {
                LOG.error("Unable to get time difference between machine and NTP server.", e);
            }
            if (timeOffset != null) {
                parentHadoopConfig.set(AccumuloExportConstants.CHILD_TIME_OFFSET_PROP, "" + timeOffset);
            }*/
        }

        try {
            if(configuration.getParentDBType() == ACCUMULO) {
                //parentStore = new AccumuloRyaStatementStore(parentHadoopConfig);
            } else {
                //Mongo
                final MongoClient client = new MongoClient(configuration.getParentHostname(), configuration.getParentPort());
                final MongoDBRyaDAO dao = new MongoDBRyaDAO(new MongoDBRdfConfiguration(parentHadoopConfig));
                parentMetadataRepo = new MongoParentMetadataRepository(client, configuration.getParentRyaInstanceName());
                parentStore = new MongoRyaStatementStore(client, configuration.getParentRyaInstanceName(), dao);
            }

            if(configuration.getChildDBType() == ACCUMULO) {
                //childStore = new AccumuloRyaStatementStore(childHadoopConfig);
            } else {
                //Mongo
                final MongoClient client = new MongoClient(configuration.getChildHostname(), configuration.getChildPort());
                final MongoDBRyaDAO dao = new MongoDBRyaDAO(new MongoDBRdfConfiguration(childHadoopConfig));
                childMetadataRepo = new MongoParentMetadataRepository(client, configuration.getChildRyaInstanceName());
                childStore = new MongoRyaStatementStore(client, configuration.getChildRyaInstanceName(), dao);
            }


            LOG.info("Starting Merge Tool");
            if(configuration.getParentDBType() == ACCUMULO && configuration.getChildDBType() == ACCUMULO) {
                /*final AccumuloRyaStatementStore childAStore = (AccumuloRyaStatementStore) childStore;
                final AccumuloRyaStatementStore parentAStore = (AccumuloRyaStatementStore) parentStore;
                final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository(childAStore.getRyaDAO());

                //do map reduce merging.
                final AccumuloMerger accumuloMerger = new AccumuloMerger(parentAStore, childAStore, accumuloParentMetadataRepository);
                accumuloMerger.runJob();*/
            } else {
                final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                    parentMetadataRepo, childMetadataRepo, new VisibilityStatementMerger(),
                    startTime, configuration.getParentRyaInstanceName());
                merger.runJob();
            }
        } catch (final NumberFormatException | RyaDAOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                LOG.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        LOG.info("Finished running Merge Tool");
        System.exit(1);
    }
}

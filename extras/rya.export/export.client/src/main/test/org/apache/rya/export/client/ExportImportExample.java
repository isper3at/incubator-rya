package org.apache.rya.export.client;
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

import static org.apache.rya.export.client.conf.MergeConfigurationCLI.DATE_FORMAT;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.parent.AccumuloParentMetadataRepository;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigHadoopAdapter;
import org.apache.rya.export.client.conf.MergeConfigurationCLI;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.apache.rya.export.mongo.time.TimeMongoRyaStatementStore;
import org.codehaus.groovy.runtime.AbstractComparator;

import com.mongodb.MongoClient;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.MongoDBRdfConfiguration;
import mvm.rya.mongodb.MongoDBRyaDAO;

public class ExportImportExample {
    private static final Logger log = Logger.getLogger(ExportImportExample.class);

    private static MergeConfiguration configuration;
    public static void main(final String [] args) throws MergeConfigurationException, ParseException, MergerException, IOException, RyaDAOException {
        final MergeConfigurationCLI config = new MergeConfigurationCLI(args);
        try {
            configuration = config.createConfiguration();
        } catch (final MergeConfigurationException e) {
            log.error("Configuration failed.", e);
        }

        final Date startTime = DATE_FORMAT.parse(configuration.getToolStartTime());

        final AccumuloMergeConfiguration accumuloConf = (AccumuloMergeConfiguration) configuration;
        final AccumuloInstanceDriver aInstance = new AccumuloInstanceDriver(
                accumuloConf.getParentRyaInstanceName(), accumuloConf.getParentInstanceType(),
                true, false, true, accumuloConf.getParentUsername(), accumuloConf.getParentPassword(),
                accumuloConf.getParentRyaInstanceName(), accumuloConf.getParentTablePrefix(),
                accumuloConf.getParentAuths(), accumuloConf.getParentZookeepers());
        try {
            aInstance.setUpInstance();
            aInstance.setUpTables();
            aInstance.setUpDao();
            aInstance.setUpConfig();
        } catch (final Exception e) {
            throw new MergerException(e);
        }
        AccumuloRyaStatementStore accumuloStore = new AccumuloRyaStatementStore(aInstance.getDao(), aInstance.getTablePrefix(), aInstance.getInstanceName());
        final AccumuloParentMetadataRepository accumuloParentMetadataRepo = new AccumuloParentMetadataRepository(aInstance.getDao());
        final String accumuloInstanceName = aInstance.getInstanceName();

        final String mongoInstanceName = configuration.getChildRyaInstanceName();
        final MongoClient client = new MongoClient(configuration.getChildHostname(), configuration.getChildPort());
        final MongoDBRyaDAO dao = new MongoDBRyaDAO(new MongoDBRdfConfiguration(MergeConfigHadoopAdapter.getMongoConfiguration(configuration)), client);
        final MongoRyaStatementStore mStore = new MongoRyaStatementStore(client, configuration.getChildRyaInstanceName(), dao);
        final TimeMongoRyaStatementStore mongoStore = new TimeMongoRyaStatementStore(mStore, startTime, mongoInstanceName);
        final MongoParentMetadataRepository mongoParentMetadataRepo = new MongoParentMetadataRepository(client, configuration.getChildRyaInstanceName());

        System.out.println("\n\n");
        pause();
        doLoadDemoData(accumuloStore, accumuloInstanceName, startTime.getTime());
        pause();
        doShowStatements(accumuloStore, accumuloInstanceName, startTime.getTime());//accumulo parent
        pause();
        accumuloStore.addIterator(getAccumuloTimestampIterator(startTime));
        doMerge("Cloning", accumuloStore, mongoStore, accumuloParentMetadataRepo, mongoParentMetadataRepo, startTime, accumuloInstanceName, mongoInstanceName);
        pause();
        doShowAllStatements(mongoStore, mongoInstanceName);//mongo child
        pause();
        doDeleteMostRecent(mongoStore, mongoInstanceName);//mongo child
        pause();
        doAddStatements(mongoStore, mongoInstanceName);//mongo child
        pause();
        doShowAllStatements(mongoStore, mongoInstanceName);//mongo child
        pause();
        doMerge("Merging", mongoStore, accumuloStore, mongoParentMetadataRepo, accumuloParentMetadataRepo, startTime, mongoInstanceName, accumuloInstanceName);
        pause();
        accumuloStore = new AccumuloRyaStatementStore(aInstance.getDao(), aInstance.getTablePrefix(), aInstance.getInstanceName());
        doShowStatements(accumuloStore, accumuloInstanceName, startTime.getTime());//accumulo parent
        pause();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                log.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        log.info("Finished running Merge Tool");
        System.exit(1);
    }

    private static void doAddStatements(final RyaStatementStore store, final String instanceName) throws AddStatementException {
        printTitle("Adding 2 statements to " + instanceName);
        printStatementHeader();

        RyaStatement statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://vessel/HAILONG08-e7c04cfa5bd0"));
        statement.setPredicate(new RyaURI("http://a"));
        statement.setObject(new RyaType("http://Vessel"));
        statement.setTimestamp(new Date().getTime());
        store.addStatement(statement);
        printStatements(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://vessel/HAILONG08-e7c04cfa5bd0"));
        statement.setPredicate(new RyaURI("http://name"));
        statement.setObject(new RyaType("http://HAI LONG 08"));
        statement.setTimestamp(new Date().getTime());
        store.addStatement(statement);
        printStatements(statement);
    }

    private static void doLoadDemoData(final RyaStatementStore store, final String instanceName, final Long time) throws AddStatementException {
        printTitle("Adding demo statements to " + instanceName);

        final List<RyaStatement> statements = new ArrayList<>();
        addVessel(statements, "HAIXUN09083", time - 100000L, "412097120", "22.660354", "113.676063333");
        addVessel(statements, "HAIXUN21", time - 90000L, "412097122", "24.8906366667", "118.6804");
        addVessel(statements, "YUZHENG_45001", time + 80000L, "412097124", "21.4887366667", "109.0819");
        addVessel(statements, "CG126", time + 70000L, "412097126", "22.6178716667", "120.28456");
        for(final RyaStatement stmnt : statements) {
            store.addStatement(stmnt);
            try {
                System.out.print(".");
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done");
    }

    private static void addVessel(final List<RyaStatement> statements, final String name,
            final long timestamp, final String seed, final String lat, final String lon) {
        RyaStatement statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://vessel/" + name + "-" + seed));
        statement.setPredicate(new RyaURI("http://a"));
        statement.setObject(new RyaType("http://Vessel"));
        statement.setTimestamp(timestamp);
        statements.add(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://vessel/" + name + "-" + seed));
        statement.setPredicate(new RyaURI("http://name"));
        statement.setObject(new RyaType("http://" + name));
        statement.setTimestamp(timestamp);
        statements.add(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://observed/" + seed));
        statement.setPredicate(new RyaURI("http://a"));
        statement.setObject(new RyaType("http://Observation"));
        statement.setTimestamp(timestamp);
        statements.add(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://observed/" + seed));
        statement.setPredicate(new RyaURI("http://vesselName"));
        statement.setObject(new RyaType("http://" + name));
        statement.setTimestamp(timestamp);
        statements.add(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://observed/" + seed));
        statement.setPredicate(new RyaURI("http://latitude"));
        statement.setObject(new RyaType("http://" + lat));
        statement.setTimestamp(timestamp);
        statements.add(statement);

        statement = new RyaStatement();
        statement.setSubject(new RyaURI("http://observed/" + seed));
        statement.setPredicate(new RyaURI("http://longitude"));
        statement.setObject(new RyaType("http://" + lon));
        statement.setTimestamp(timestamp);
        statements.add(statement);
    }

    private static void doShowStatements(final RyaStatementStore store, final String instanceName, final Long startTime) throws FetchStatementException {
        printTitle("Showing all statements from " + instanceName);
        final Iterator<RyaStatement> statements = store.fetchStatements();
        printStatementHeader();
        final List<RyaStatement> stmnts = new ArrayList<>();
        while(statements.hasNext()) {
            stmnts.add(statements.next());
        }
        stmnts.sort(new AbstractComparator<RyaStatement>() {
            @Override
            public int compare(final RyaStatement stmnt1, final RyaStatement stmnt2) {
                return stmnt1.getTimestamp().compareTo(stmnt2.getTimestamp());
            }
        });

        boolean shown = false;
        for(final RyaStatement stmnt : stmnts) {
            if(stmnt.getTimestamp() >= startTime && !shown) {
                System.out.printf("|   %96s                       |\n", "Statements below are after the selected timestamp.");
                shown = true;
            }
            printStatements(stmnt);
        }
    }

    private static void doShowAllStatements(final RyaStatementStore store, final String instanceName) throws FetchStatementException {
        printTitle("Showing all statements from " + instanceName);
        printStatementHeader();
        final Iterator<RyaStatement> statements = store.fetchStatements();
        while(statements.hasNext()) {
            final RyaStatement stmnt = statements.next();
            printStatements(stmnt);
        }
    }

    private static void doMerge(final String title, final RyaStatementStore parentStore, final RyaStatementStore childStore,
            final ParentMetadataRepository parentMetadata, final ParentMetadataRepository childMetadata, final Date startTime,
            final String parentInstance, final String childInstance) {
        printTitle(title + " all statements after: " + DATE_FORMAT.format(startTime) + " from " + parentInstance + " to " + childInstance);
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            parentMetadata, childMetadata, new VisibilityStatementMerger(),
            startTime, configuration.getParentRyaInstanceName(), 0L);
        merger.runJob();
        log.info("...Done.");
    }

    private static void doDeleteMostRecent(final RyaStatementStore store, final String instanceName) throws FetchStatementException, RemoveStatementException {
        printTitle("Deleting the most recent statement from " + instanceName);
        final Iterator<RyaStatement> statements = store.fetchStatements();
        RyaStatement lastStatement = null;
        while(statements.hasNext()) {
            lastStatement = statements.next();
        }
        System.out.println("Deleting: ");
        printStatementHeader();
        printStatements(lastStatement);
        store.removeStatement(lastStatement);
    }

    private static IteratorSetting getAccumuloTimestampIterator(final Date startTime) {
        final IteratorSetting setting = new IteratorSetting(1, "startTimeIterator", TimestampFilter.class);
        TimestampFilter.setStart(setting, startTime.getTime(), true);
        TimestampFilter.setEnd(setting, Long.MAX_VALUE, true);
        return setting;
    }

    private static void pause() throws IOException {
        System.out.println("Press [Enter] to continue.");
        System.in.read();
    }

    private static void printTitle(final String title) {
        final int size = title.length() + 6;
        System.out.printf("|%s|\n", StringUtils.center("", size, "-"));
        System.out.printf("|%s|\n", StringUtils.center(title, size, " "));
        System.out.printf("|%s|\n", StringUtils.center("", size, "-"));
    }

    private static void printStatementHeader() {
        System.out.println("|                   Subject                 |       Predicate       |         Object           |         Timestamp         |");
        System.out.println("|--------------------------------------------------------------------------------------------------------------------------|");
    }

    private static void printStatements(final RyaStatement stmnt) {
        System.out.printf("|   %37s   |   %17s   |   %20s   |   %s   |\n", stmnt.getSubject().getData(), stmnt.getPredicate().getData(), stmnt.getObject().getData(), DATE_FORMAT.format(new Date(stmnt.getTimestamp())));
    }
}

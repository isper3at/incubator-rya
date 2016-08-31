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

import static org.apache.rya.export.accumulo.TestUtils.LAST_MONTH;
import static org.apache.rya.export.accumulo.TestUtils.TODAY;
import static org.apache.rya.export.accumulo.TestUtils.YESTERDAY;
import static org.apache.rya.export.accumulo.TestUtils.createRyaStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.apache.rya.export.CopyType;
import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.driver.AccumuloDualInstanceDriver;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.accumulo.util.AccumuloRyaUtils;
import org.apache.rya.export.accumulo.util.TimeUtils;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests the methods of {@link AccumuloCopier}.
 */
public class AccumuloCopierTest {
    private static final Logger log = Logger.getLogger(AccumuloCopierTest.class);

    private static final InstanceType INSTANCE_TYPE = InstanceType.MOCK;

    private static final boolean IS_MOCK = INSTANCE_TYPE.isMock();
    private static final boolean USE_TIME_SYNC = false;
    private static final boolean USE_COPY_FILE_OUTPUT = false;
    private static final boolean IS_START_TIME_DIALOG_ENABLED = false;

    private static final String PARENT_PASSWORD = AccumuloDualInstanceDriver.PARENT_PASSWORD;
    private static final String PARENT_INSTANCE = AccumuloDualInstanceDriver.PARENT_INSTANCE;
    private static final String PARENT_TABLE_PREFIX = AccumuloDualInstanceDriver.PARENT_TABLE_PREFIX;
    private static final String PARENT_AUTH = AccumuloDualInstanceDriver.PARENT_AUTH;
    private static final ColumnVisibility PARENT_COLUMN_VISIBILITY = new ColumnVisibility(PARENT_AUTH);
    private static final String PARENT_TOMCAT_URL = "http://rya-example-box:8080";

    private static final String CHILD_PASSWORD = AccumuloDualInstanceDriver.CHILD_PASSWORD;
    private static final String CHILD_INSTANCE = AccumuloDualInstanceDriver.CHILD_INSTANCE;
    private static final String CHILD_TABLE_PREFIX = AccumuloDualInstanceDriver.CHILD_TABLE_PREFIX;
    private static final String CHILD_AUTH = AccumuloDualInstanceDriver.CHILD_AUTH;
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";

    private CopyType copyType = CopyType.CONNECTED_DATASTORES;

    private static AccumuloRyaDAO parentDao;
    private static AccumuloRyaDAO childDao;

    private static AccumuloRdfConfiguration parentConfig;
    private static AccumuloRdfConfiguration childConfig;

    private static AccumuloDualInstanceDriver accumuloDualInstanceDriver;
    //private static CopyTool copyTool = null;
    private boolean isImporting = false;

    @BeforeClass
    public static void setUp() throws Exception {
        accumuloDualInstanceDriver = new AccumuloDualInstanceDriver(INSTANCE_TYPE, true, true, false, false);
        accumuloDualInstanceDriver.setUpInstances();
    }

    @Before
    public void setUpPerTest() throws Exception {
        accumuloDualInstanceDriver.setUpTables();

        accumuloDualInstanceDriver.setUpDaos();

        accumuloDualInstanceDriver.setUpConfigs();

        parentConfig = accumuloDualInstanceDriver.getParentConfig();
        childConfig = accumuloDualInstanceDriver.getChildConfig();
        parentDao = accumuloDualInstanceDriver.getParentDao();
        childDao = accumuloDualInstanceDriver.getChildDao();
    }

    @After
    public void tearDownPerTest() throws Exception {
        log.info("tearDownPerTest(): tearing down now.");
        accumuloDualInstanceDriver.tearDownTables();
        accumuloDualInstanceDriver.tearDownDaos();
    }

    @AfterClass
    public static void tearDownPerClass() throws Exception {
        log.info("tearDownPerClass(): tearing down now.");
        accumuloDualInstanceDriver.tearDown();
    }

    private void assertStatementInChild(final String description, final int verifyResultCount, final RyaStatement matchStatement) throws RyaDAOException {
        TestUtils.assertStatementInInstance(description, verifyResultCount, matchStatement, childDao, childConfig);
    }

    private AccumuloCopier copyToolRun(final Date startDate) throws AccumuloException, AccumuloSecurityException, MergerException {
        copyType = isImporting ? CopyType.FILE_IMPORT : USE_COPY_FILE_OUTPUT ? CopyType.FILE_EXPORT : CopyType.CONNECTED_DATASTORES;

        final AccumuloMergeConfiguration accumuloMergeConfiguration = mock(AccumuloMergeConfiguration.class);
        // Parent
        when(accumuloMergeConfiguration.getParentRyaInstanceName()).thenReturn(PARENT_INSTANCE);
        when(accumuloMergeConfiguration.getParentUsername()).thenReturn(accumuloDualInstanceDriver.getParentUser());
        when(accumuloMergeConfiguration.getParentPassword()).thenReturn(PARENT_PASSWORD);
        when(accumuloMergeConfiguration.getParentInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getParentTablePrefix()).thenReturn(PARENT_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getParentAuths()).thenReturn(PARENT_AUTH);
        when(accumuloMergeConfiguration.getParentAuths()).thenReturn(PARENT_AUTH);
        when(accumuloMergeConfiguration.getParentTomcatUrl()).thenReturn(PARENT_TOMCAT_URL);
        when(accumuloMergeConfiguration.getParentDBType()).thenReturn(DBType.ACCUMULO);
        // Child
        when(accumuloMergeConfiguration.getChildRyaInstanceName()).thenReturn(CHILD_INSTANCE);
        when(accumuloMergeConfiguration.getChildUsername()).thenReturn(copyType == CopyType.FILE_EXPORT ? AccumuloInstanceDriver.ROOT_USER_NAME : accumuloDualInstanceDriver.getChildUser());
        when(accumuloMergeConfiguration.getChildPassword()).thenReturn(CHILD_PASSWORD);
        when(accumuloMergeConfiguration.getChildInstanceType()).thenReturn(copyType == CopyType.FILE_IMPORT ? INSTANCE_TYPE : copyType == CopyType.FILE_EXPORT ? InstanceType.MINI : INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getChildTablePrefix()).thenReturn(CHILD_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getChildAuths()).thenReturn(CHILD_AUTH);
        when(accumuloMergeConfiguration.getChildTomcatUrl()).thenReturn(CHILD_TOMCAT_URL);
        when(accumuloMergeConfiguration.getChildDBType()).thenReturn(DBType.ACCUMULO);
        // Other
        when(accumuloMergeConfiguration.getMergePolicy()).thenReturn(MergePolicy.TIMESTAMP);
        when(accumuloMergeConfiguration.getCopyType()).thenReturn(copyType);
        when(accumuloMergeConfiguration.getOutputPath()).thenReturn("/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getImportPath()).thenReturn("resources/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getNtpServerHost()).thenReturn(TimeUtils.DEFAULT_TIME_SERVER_HOST);
        when(accumuloMergeConfiguration.getUseNtpServer()).thenReturn(USE_TIME_SYNC);
        when(accumuloMergeConfiguration.getToolStartTime()).thenReturn(AccumuloExportConstants.convertDateToStartTimeString(new Date()));

        final AccumuloRyaStatementStore accumuloParentRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, true);
        final AccumuloRyaStatementStore childAccumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, false);

        if (startDate != null) {
            final IteratorSetting startTimeIteratorSetting = AccumuloExportConstants.getStartTimeSetting(startDate);
            accumuloParentRyaStatementStore.addIterator(startTimeIteratorSetting);
            childAccumuloRyaStatementStore.addIterator(startTimeIteratorSetting);
        }

        final MergeParentMetadata mergeParentMetadata = new MergeParentMetadata(PARENT_INSTANCE, startDate, null, 0L);
        final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository(accumuloParentRyaStatementStore.getRyaDAO());
        accumuloParentMetadataRepository.set(mergeParentMetadata);

        final AccumuloCopier accumuloCopier = new AccumuloCopier(accumuloMergeConfiguration, accumuloParentRyaStatementStore, childAccumuloRyaStatementStore, accumuloParentMetadataRepository);
        accumuloCopier.runJob();

        log.info("Finished running tool.");

        return accumuloCopier;
    }

    @Test
    public void testCopyTool() throws Exception {
        final RyaStatement ryaStatementOutOfTimeRange = createRyaStatement("coach", "called", "timeout", LAST_MONTH);

        final RyaStatement ryaStatementShouldCopy1 = createRyaStatement("bob", "catches", "ball", YESTERDAY);
        final RyaStatement ryaStatementShouldCopy2 = createRyaStatement("bill", "talks to", "john", YESTERDAY);
        final RyaStatement ryaStatementShouldCopy3 = createRyaStatement("susan", "eats", "burgers", TODAY);
        final RyaStatement ryaStatementShouldCopy4 = createRyaStatement("ronnie", "plays", "guitar", TODAY);

        final RyaStatement ryaStatementDoesNotExist1 = createRyaStatement("nobody", "was", "here", LAST_MONTH);
        final RyaStatement ryaStatementDoesNotExist2 = createRyaStatement("statement", "not", "found", YESTERDAY);
        final RyaStatement ryaStatementDoesNotExist3 = createRyaStatement("key", "does not", "exist", TODAY);

        // This statement was modified by the child to change the column visibility.
        // The parent should combine the child's visibility with its visibility.
        final RyaStatement ryaStatementVisibilityDifferent = createRyaStatement("I", "see", "you", YESTERDAY);
        ryaStatementVisibilityDifferent.setColumnVisibility(PARENT_COLUMN_VISIBILITY.getExpression());

        // Setup initial parent instance with 7 rows
        // This is the state of the parent data (as it is today) before merging occurs which will use the specified start time of yesterday.
        parentDao.add(ryaStatementOutOfTimeRange);      // Process should NOT copy statement
        parentDao.add(ryaStatementShouldCopy1);         // Process should copy statement
        parentDao.add(ryaStatementShouldCopy2);         // Process should copy statement
        parentDao.add(ryaStatementShouldCopy3);         // Process should copy statement
        parentDao.add(ryaStatementShouldCopy4);         // Process should copy statement
        parentDao.add(ryaStatementVisibilityDifferent); // Process should copy and update statement


        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        //AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        log.info("Starting copy tool. Copying all data after the specified start time: " + YESTERDAY);

        isImporting = false;
        final AccumuloCopier accumuloCopier = copyToolRun(YESTERDAY);


        // Copy Tool made child instance so hook the tables and dao into the driver.
        final String childUser = accumuloDualInstanceDriver.getChildUser();
        final Connector childConnector = ConfigUtils.getConnector(childConfig);
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setConnector(childConnector);

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpTables();

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpDao();
        childDao = accumuloDualInstanceDriver.getChildDao();


        // Update child config to include changes made from copy process
        final SecurityOperations childSecOps = accumuloDualInstanceDriver.getChildSecOps();
        Authorizations newChildAuths = AccumuloRyaUtils.addUserAuths(childUser, childSecOps, PARENT_AUTH);
        childSecOps.changeUserAuthorizations(childUser, newChildAuths);
        final String childAuthString = newChildAuths.toString();
        final List<String> duplicateKeys = AccumuloExportConstants.DUPLICATE_KEY_MAP.get(MRUtils.AC_AUTH_PROP);
        childConfig.set(MRUtils.AC_AUTH_PROP, childAuthString);
        for (final String key : duplicateKeys) {
            childConfig.set(key, childAuthString);
        }


        if (USE_COPY_FILE_OUTPUT) {
            // Set up the child tables now to test importing the files back into the child instance
            final String childTableName = CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
            try {
                accumuloCopier.createTableIfNeeded(childTableName);
                accumuloCopier.importFilesToChildTable(childTableName, childConfig);
            } catch (final Exception e) {
                log.error("Failed to import files into child instance.", e);
            }
        }

        AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, childConfig);
        AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, childConfig);
        AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        final Scanner scanner = AccumuloRyaUtils.getScanner(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        // Make sure we have all of them in the parent.
        assertEquals(5, count);


        assertStatementInChild("Child included statement that was out of time range", 0, ryaStatementOutOfTimeRange);

        assertStatementInChild("Child missing statement 1 that was in parent", 1, ryaStatementShouldCopy1);

        assertStatementInChild("Child missing statement 2 that was in parent", 1, ryaStatementShouldCopy2);

        assertStatementInChild("Child missing statement 3 that was in parent", 1, ryaStatementShouldCopy3);

        assertStatementInChild("Child missing statement 4 that was in parent", 1, ryaStatementShouldCopy4);

        assertStatementInChild("Child included statement 1 that was not in parent", 0, ryaStatementDoesNotExist1);

        assertStatementInChild("Child included statement 2 that was not in parent", 0, ryaStatementDoesNotExist2);

        assertStatementInChild("Child included statement 3 that was not in parent", 0, ryaStatementDoesNotExist3);

        // Check that it can be queried with child's visibility
        assertStatementInChild("Child missing statement with child visibility", 1, ryaStatementVisibilityDifferent);

        // Check that it can be queried with parent's visibility
        childConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, PARENT_AUTH);
        final SecurityOperations secOps = IS_MOCK ? accumuloDualInstanceDriver.getChildSecOps() : childSecOps;
        newChildAuths = AccumuloRyaUtils.addUserAuths(accumuloDualInstanceDriver.getChildUser(), secOps, PARENT_AUTH);
        secOps.changeUserAuthorizations(accumuloDualInstanceDriver.getChildUser(), newChildAuths);
        assertStatementInChild("Child missing statement with parent visibility", 1, ryaStatementVisibilityDifferent);

        // Check that it can NOT be queried with some other visibility
        childConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "bad_auth");
        final CloseableIteration<RyaStatement, RyaDAOException> iter = childDao.getQueryEngine().query(ryaStatementVisibilityDifferent, childConfig);
        count = 0;
        try {
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        } catch (final Exception e) {
            // Expected
            if (!(e.getCause() instanceof AccumuloSecurityException)) {
                fail();
            }
        }
        iter.close();
        assertEquals(0, count);

        // reset auth
        childConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, childAuthString);

        log.info("DONE");
    }

    @Test
    public void testImportDirectoryTool() throws Exception {
        log.info("");
        log.info("Setting up initial state of parent before importing directory to child...");
        log.info("Adding data to parent...");

        log.info("Starting import directory tool. Importing all data after the specified start time: " + YESTERDAY);
        log.info("");

        isImporting = true;
        copyToolRun(YESTERDAY);


        // Import Directory Tool made child instance so hook the tables and dao into the driver.
        final String childUser = accumuloDualInstanceDriver.getChildUser();
        final Connector childConnector = ConfigUtils.getConnector(childConfig);
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setConnector(childConnector);

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpTables();

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpDao();


        // Update child config to include changes made from import directory process
        final SecurityOperations childSecOps = accumuloDualInstanceDriver.getChildSecOps();
        final Authorizations newChildAuths = AccumuloRyaUtils.addUserAuths(childUser, childSecOps, PARENT_AUTH);
        childSecOps.changeUserAuthorizations(childUser, newChildAuths);
        final String childAuthString = newChildAuths.toString();
        final List<String> duplicateKeys = AccumuloExportConstants.DUPLICATE_KEY_MAP.get(MRUtils.AC_AUTH_PROP);
        childConfig.set(MRUtils.AC_AUTH_PROP, childAuthString);
        for (final String key : duplicateKeys) {
            childConfig.set(key, childAuthString);
        }


        //AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, childConfig);
        //AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, childConfig);
        AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        final Scanner scanner = AccumuloRyaUtils.getScanner(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        log.info("");
        log.info("Total rows imported: " + count);
        log.info("");

        assertEquals(20, count);

        log.info("DONE");
    }
}

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
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.export.CopyType;
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
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

/**
 * Tests the methods of {@link AccumuloMerger}.
 */
public class AccumuloMergerTest {
    private static final Logger log = Logger.getLogger(AccumuloMergerTest.class);

    private static final InstanceType INSTANCE_TYPE = InstanceType.MOCK;

    private static final boolean IS_MOCK = INSTANCE_TYPE.isMock();
    private static final boolean USE_TIME_SYNC = false;
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
    private static final ColumnVisibility CHILD_COLUMN_VISIBILITY = new ColumnVisibility(CHILD_AUTH);
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";

    private static Connector parentConnector;
    private static Connector childConnector;

    private static AccumuloRyaDAO parentDao;
    private static AccumuloRyaDAO childDao;

    private static AccumuloRdfConfiguration parentConfig;
    private static AccumuloRdfConfiguration childConfig;

    private static AccumuloDualInstanceDriver accumuloDualInstanceDriver;

    @Test
    public void testRunJob() throws Exception {
        final AccumuloMergeConfiguration accumuloMergeConfiguration = mock(AccumuloMergeConfiguration.class);
        // Parent
        when(accumuloMergeConfiguration.getParentRyaInstanceName()).thenReturn(PARENT_INSTANCE);
        when(accumuloMergeConfiguration.getParentUsername()).thenReturn(accumuloDualInstanceDriver.getParentUser());
        when(accumuloMergeConfiguration.getParentPassword()).thenReturn(PARENT_PASSWORD);
        when(accumuloMergeConfiguration.getParentInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getParentTablePrefix()).thenReturn(PARENT_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getParentAuths()).thenReturn(PARENT_AUTH);
        // Child
        when(accumuloMergeConfiguration.getChildRyaInstanceName()).thenReturn(CHILD_INSTANCE);
        when(accumuloMergeConfiguration.getChildUsername()).thenReturn(accumuloDualInstanceDriver.getChildUser());
        when(accumuloMergeConfiguration.getChildPassword()).thenReturn(CHILD_PASSWORD);
        when(accumuloMergeConfiguration.getChildInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getChildTablePrefix()).thenReturn(CHILD_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getChildAuths()).thenReturn(CHILD_AUTH);
        // Other
        when(accumuloMergeConfiguration.getMergePolicy()).thenReturn(MergePolicy.TIMESTAMP);
        when(accumuloMergeConfiguration.getCopyType()).thenReturn(CopyType.CONNECTED_DATASTORES);
        when(accumuloMergeConfiguration.getOutputPath()).thenReturn("/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getImportPath()).thenReturn("resources/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getNtpServerHost()).thenReturn(TimeUtils.DEFAULT_TIME_SERVER_HOST);
        when(accumuloMergeConfiguration.getUseNtpServer()).thenReturn(USE_TIME_SYNC);
        when(accumuloMergeConfiguration.getToolStartTime()).thenReturn(AccumuloExportConstants.convertDateToStartTimeString(new Date()));

        final AccumuloRyaStatementStore accumuloParentRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, true);
        final AccumuloRyaStatementStore childAccumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, false);

        final MergeParentMetadata mergeParentMetadata = new MergeParentMetadata(PARENT_INSTANCE, new Date(), null, 0L);
        final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository(accumuloParentRyaStatementStore.getRyaDAO());
        accumuloParentMetadataRepository.set(mergeParentMetadata);

        final AccumuloMerger accumuloMerger = new AccumuloMerger(accumuloMergeConfiguration, accumuloParentRyaStatementStore, childAccumuloRyaStatementStore, accumuloParentMetadataRepository);
        accumuloMerger.runJob();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        accumuloDualInstanceDriver = new AccumuloDualInstanceDriver(INSTANCE_TYPE, false, false, true, true);
        accumuloDualInstanceDriver.setUpInstances();

        parentConnector = accumuloDualInstanceDriver.getParentConnector();
        childConnector = accumuloDualInstanceDriver.getChildConnector();
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

    private void assertStatementInParent(final String description, final int verifyResultCount, final RyaStatement matchStatement) throws RyaDAOException {
        TestUtils.assertStatementInInstance(description, verifyResultCount, matchStatement, parentDao, parentConfig);
    }

    private void mergeToolRun(final Date startDate) throws MergerException {
        final AccumuloMergeConfiguration accumuloMergeConfiguration = mock(AccumuloMergeConfiguration.class);
        // Parent
        when(accumuloMergeConfiguration.getParentRyaInstanceName()).thenReturn(PARENT_INSTANCE);
        when(accumuloMergeConfiguration.getParentUsername()).thenReturn(accumuloDualInstanceDriver.getParentUser());
        when(accumuloMergeConfiguration.getParentPassword()).thenReturn(PARENT_PASSWORD);
        when(accumuloMergeConfiguration.getParentInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getParentTablePrefix()).thenReturn(PARENT_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getParentAuths()).thenReturn(PARENT_AUTH);
        when(accumuloMergeConfiguration.getParentTomcatUrl()).thenReturn(PARENT_TOMCAT_URL);
        // Child
        when(accumuloMergeConfiguration.getChildRyaInstanceName()).thenReturn(CHILD_INSTANCE);
        when(accumuloMergeConfiguration.getChildUsername()).thenReturn(accumuloDualInstanceDriver.getChildUser());
        when(accumuloMergeConfiguration.getChildPassword()).thenReturn(CHILD_PASSWORD);
        when(accumuloMergeConfiguration.getChildInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getChildTablePrefix()).thenReturn(CHILD_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getChildAuths()).thenReturn(CHILD_AUTH);
        when(accumuloMergeConfiguration.getChildTomcatUrl()).thenReturn(CHILD_TOMCAT_URL);
        // Other
        when(accumuloMergeConfiguration.getMergePolicy()).thenReturn(MergePolicy.TIMESTAMP);
        when(accumuloMergeConfiguration.getCopyType()).thenReturn(CopyType.CONNECTED_DATASTORES);
        when(accumuloMergeConfiguration.getOutputPath()).thenReturn("/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getImportPath()).thenReturn("resources/test/copy_tool_file_output/");
        when(accumuloMergeConfiguration.getNtpServerHost()).thenReturn(TimeUtils.DEFAULT_TIME_SERVER_HOST);
        when(accumuloMergeConfiguration.getUseNtpServer()).thenReturn(USE_TIME_SYNC);
        when(accumuloMergeConfiguration.getToolStartTime()).thenReturn(AccumuloExportConstants.convertDateToStartTimeString(new Date()));


        final AccumuloRyaStatementStore accumuloParentRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, true);
        final AccumuloRyaStatementStore childAccumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloMergeConfiguration, false);

        final MergeParentMetadata mergeParentMetadata = new MergeParentMetadata(PARENT_INSTANCE, startDate, null, 0L);
        final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository(accumuloParentRyaStatementStore.getRyaDAO());
        accumuloParentMetadataRepository.set(mergeParentMetadata);

        final AccumuloMerger accumuloMerger = new AccumuloMerger(accumuloMergeConfiguration, accumuloParentRyaStatementStore, childAccumuloRyaStatementStore, accumuloParentMetadataRepository);
        accumuloMerger.runJob();

        log.info("Finished running tool.");
    }

    @Test
    public void testMergeTool() throws Exception {
        // This statement was in both parent/child instances a month ago and is before the start time of yesterday
        // but it was left alone.  It should remain in the parent after merging.
        final RyaStatement ryaStatementOutOfTimeRange = createRyaStatement("coach", "called", "timeout", LAST_MONTH);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the parent deleted it and the child still has it.  It should stay deleted in the parent after merging.
        final RyaStatement ryaStatementParentDeletedAfter = createRyaStatement("parent", "deleted", "after", LAST_MONTH);

        // This statement was added by the parent after the start time of yesterday and doesn't exist in the child.
        // It should stay in the parent after merging.
        final RyaStatement ryaStatementParentAddedAfter = createRyaStatement("parent", "added", "after", TODAY);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the child deleted it and the parent still has it.  It should be deleted from the parent after merging.
        final RyaStatement ryaStatementChildDeletedAfter = createRyaStatement("child", "deleted", "after", LAST_MONTH);

        // This statement was added by the child after the start time of yesterday and doesn't exist in the parent.
        // It should be added to the parent after merging.
        final RyaStatement ryaStatementChildAddedAfter = createRyaStatement("child", "added", "after", TODAY);

        // This statement was modified by the child after the start of yesterday (The timestamp changes after updating)
        // It should be updated in the parent to match the child.
        final RyaStatement ryaStatementUpdatedByChild = createRyaStatement("bob", "catches", "ball", LAST_MONTH);

        final RyaStatement ryaStatementUntouchedByChild = createRyaStatement("bill", "talks to", "john", LAST_MONTH);

        final RyaStatement ryaStatementDeletedByChild = createRyaStatement("susan", "eats", "burgers", LAST_MONTH);

        final RyaStatement ryaStatementAddedByChild = createRyaStatement("ronnie", "plays", "guitar", TODAY);

        // This statement was modified by the child to change the column visibility.
        // The parent should combine the child's visibility with its visibility.
        final RyaStatement ryaStatementVisibilityDifferent = createRyaStatement("I", "see", "you", LAST_MONTH);
        ryaStatementVisibilityDifferent.setColumnVisibility(PARENT_COLUMN_VISIBILITY.getExpression());

        // Setup initial parent instance with 7 rows
        // This is the state of the parent data (as it is today) before merging occurs which will use the specified start time of yesterday.
        parentDao.add(ryaStatementOutOfTimeRange);      // Merging should keep statement
        parentDao.add(ryaStatementUpdatedByChild);      // Merging should update statement
        parentDao.add(ryaStatementUntouchedByChild);    // Merging should keep statement
        parentDao.add(ryaStatementDeletedByChild);      // Merging should delete statement
        parentDao.add(ryaStatementVisibilityDifferent); // Merging should update statement
        parentDao.add(ryaStatementParentAddedAfter);    // Merging should keep statement
        parentDao.add(ryaStatementChildDeletedAfter);   // Merging should delete statement

        // Simulate the child coming back with a modified data set before the merging occurs.
        // (1 updated row, 1 row left alone because it was unchanged, 1 row outside time range,
        // 1 row deleted, 1 new row added, 1 modified visibility, 1 deleted by child, 1 added by child).
        // There should be 5 rows in the child instance (4 which will be scanned over from the start time).
        ryaStatementUpdatedByChild.setObject(TestUtils.createRyaUri("football"));
        ryaStatementUpdatedByChild.setTimestamp(TODAY.getTime());
        ryaStatementVisibilityDifferent.setColumnVisibility(CHILD_COLUMN_VISIBILITY.getExpression());
        childDao.add(ryaStatementOutOfTimeRange);
        childDao.add(ryaStatementUpdatedByChild);
        childDao.add(ryaStatementUntouchedByChild);
        childDao.add(ryaStatementAddedByChild);         // Merging should add statement
        childDao.add(ryaStatementVisibilityDifferent);
        childDao.add(ryaStatementParentDeletedAfter);
        childDao.add(ryaStatementChildAddedAfter);      // Merging should add statement

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        log.info("Starting merge tool. Merging all data after the specified start time: " + YESTERDAY);

        mergeToolRun(YESTERDAY);


        for (final String tableSuffix : AccumuloInstanceDriver.TABLE_NAME_SUFFIXES) {
            AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + tableSuffix, parentConfig);
        }

        final Scanner scanner = AccumuloRyaUtils.getScanner(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        // Make sure we have all of them in the parent.
        assertEquals(7, count);


        assertStatementInParent("Parent missing statement that untouched by the child", 1, ryaStatementUntouchedByChild);

        assertStatementInParent("Parent missing statement that was out of time range", 1, ryaStatementOutOfTimeRange);

        assertStatementInParent("Parent missing statement that was updated by the child", 1, ryaStatementUpdatedByChild);

        assertStatementInParent("Parent missing statement that was added by the child", 1, ryaStatementAddedByChild);

        assertStatementInParent("Parent has statement that the child deleted", 0, ryaStatementDeletedByChild);

        // Check that it can be queried with parent's visibility
        assertStatementInParent("Parent missing statement with parent visibility", 1, ryaStatementVisibilityDifferent);

        // Check that it can be queried with child's visibility
        parentConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, CHILD_AUTH);
        final Authorizations newParentAuths = AccumuloRyaUtils.addUserAuths(accumuloDualInstanceDriver.getParentUser(), accumuloDualInstanceDriver.getParentSecOps(), CHILD_AUTH);
        accumuloDualInstanceDriver.getParentSecOps().changeUserAuthorizations(accumuloDualInstanceDriver.getParentUser(), newParentAuths);
        assertStatementInParent("Parent missing statement with child visibility", 1, ryaStatementVisibilityDifferent);

        // Check that it can NOT be queried with some other visibility
        parentConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "bad_auth");
        final CloseableIteration<RyaStatement, RyaDAOException> iter = parentDao.getQueryEngine().query(ryaStatementVisibilityDifferent, parentConfig);
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
        parentConfig.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, PARENT_AUTH);

        assertStatementInParent("Parent has statement it deleted later", 0, ryaStatementParentDeletedAfter);

        assertStatementInParent("Parent missing statement it added later", 1, ryaStatementParentAddedAfter);

        assertStatementInParent("Parent has statement child deleted later", 0, ryaStatementChildDeletedAfter);

        assertStatementInParent("Parent missing statement child added later", 1, ryaStatementChildAddedAfter);


        log.info("DONE");
    }

    private static RyaStatement createRyaStatementUnique(final String s, final String p, final String o, final Date date) throws Exception {
        final String uniquePart = Long.toString(System.currentTimeMillis() & 0xffffff, 64);
        return createRyaStatement(s+uniquePart, p+uniquePart, o+uniquePart, date);
    }

    private static RyaStatement createRyaStatementUniqueAdd(final String s, final String p, final String o, final Date date, final AccumuloRyaDAO dao1, final AccumuloRyaDAO dao2) throws Exception {
        final String uniquePart = Long.toString(System.currentTimeMillis() & 0xffffff, 64);
        final RyaStatement rs = createRyaStatement(s + uniquePart, p + uniquePart, o + uniquePart, date);
        if (dao1 != null) {
            dao1.add(rs);
        }
        if (dao2 != null) {
            dao2.add(rs);
        }
        return rs;
    }

    @Test
    public void testMissingParentNewChild() throws Exception {
        final RyaStatement stmtNewInChild = createRyaStatementUnique("s_newInChild", "p_newInChild", "o_newInChild", null);
        final RyaStatement stmtSameInBoth = createRyaStatementUnique("s_same", "p_same", "o_same", LAST_MONTH);
        childDao.add(stmtNewInChild);      // Merging should add statement to parent
        childDao.add(stmtSameInBoth);      // Merging should ignore statement
        parentDao.add(stmtSameInBoth);     // Merging should ignore statement
        mergeToolRun(YESTERDAY);
        assertStatementInParent("new child statement added in parent ", 1, stmtNewInChild);
        assertStatementInParent("Statement in p and child. ", 1, stmtSameInBoth);      // Merging should ignore statement
    }

    @Test
    public void testOldParentMissingChild() throws Exception {
        final RyaStatement stmtMissingInChildOld = createRyaStatementUniqueAdd("s_notInChild", "p_notInChild", "o_notInChild", LAST_MONTH, parentDao, null);
        mergeToolRun(YESTERDAY);
        assertStatementInParent("Missing in child statement deleted old in parent ", 0, stmtMissingInChildOld);
    }

    @Test
    public void testNewParentEmptyChild() throws Exception {
        final RyaStatement stmtNewP_MisC = createRyaStatementUniqueAdd("s_NewP_MisC", "p_NewP_MisC", "o_NewP_MisC", null, parentDao, null);
        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        mergeToolRun(YESTERDAY);
        // Note that nothing changes.  This should catch issues with empty tables.
        assertStatementInParent("Missing in child statement should be kept new in parent ", 1, stmtNewP_MisC);
    }

    @Test
    public void testNewParentMissingChild() throws Exception {
        final RyaStatement stmtNewP_MisC = createRyaStatementUniqueAdd("s_NewP_MisC", "p_NewP_MisC", "o_NewP_MisC", null, parentDao, null);
        final RyaStatement stmtOldP_OldC = createRyaStatementUniqueAdd("s_OldP_OldC", "p_OldP_OldC", "o_OldP_OldC", LAST_MONTH, parentDao, childDao);

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        mergeToolRun(YESTERDAY);
        assertStatementInParent("Missing in child statement should be kept new in parent ", 1, stmtNewP_MisC);
        assertStatementInParent("Statement in parent and child. ", 1, stmtOldP_OldC);
    }

    @Test
    public void testEmptyParentNewChild() throws Exception {
        final RyaStatement stmtMisP_NewC_addP_z = createRyaStatementUniqueAdd("zs_MisP_NewC", "zp_MisP_NewC", "zo_MisP_NewC", null     , null     , childDao);

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        mergeToolRun(YESTERDAY);

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        assertStatementInParent("Missing Parent New Child should add Parent.", 1, stmtMisP_NewC_addP_z);

    }

    /**
     * Test all cases with statements in different splits.
     * @throws Exception
     */
    @Test
    public void testWithDefaultSplits() throws Exception {
        addAndVerifySplitableStatements();
    }

    @Test
    public void testWithParentSplits() throws Exception {
        // set splits, 4 tablets created: <b b*-g g*-v >v
        final TreeSet<Text> splits = new TreeSet<Text>();
        splits.add(new Text("b"));
        splits.add(new Text("g"));
        splits.add(new Text("v"));
        for (final String tableSuffix : AccumuloInstanceDriver.TABLE_NAME_SUFFIXES) {
            parentConnector.tableOperations().addSplits(PARENT_TABLE_PREFIX + tableSuffix, splits);
        }
        addAndVerifySplitableStatements();
    }

    @Test
    public void testWithChildSplits() throws Exception {
        // set splits, 4 tablets created: <b b*-g g*-v >v
        final TreeSet<Text> splits = new TreeSet<Text>();
        splits.add(new Text("b"));
        splits.add(new Text("g"));
        splits.add(new Text("v"));
        for (final String tableSuffix : AccumuloInstanceDriver.TABLE_NAME_SUFFIXES) {
            childConnector.tableOperations().addSplits(CHILD_TABLE_PREFIX + tableSuffix, splits);
        }
        addAndVerifySplitableStatements();
    }

    @Test
    public void testWithParentAndChildSplits() throws Exception {
        // set splits, 4 tablets created: <b b*-g g*-v >v
        final TreeSet<Text> splits = new TreeSet<Text>();
        splits.add(new Text("b"));
        splits.add(new Text("g"));
        splits.add(new Text("v"));
        for (final String tableSuffix : AccumuloInstanceDriver.TABLE_NAME_SUFFIXES) {
            parentConnector.tableOperations().addSplits(PARENT_TABLE_PREFIX + tableSuffix, splits);
        }
        for (final String tableSuffix : AccumuloInstanceDriver.TABLE_NAME_SUFFIXES) {
            childConnector.tableOperations().addSplits(CHILD_TABLE_PREFIX + tableSuffix, splits);
        }
        addAndVerifySplitableStatements();
    }

    /**
     * Not a test, but a setup for all the split tests, all cases.
     *   Parent   | Child   | assume that    | merge modification
     *   -------- | ------- | -------------- | -------------------
     *   older    | missing | child deleted  | delete from parent
     *   newer    | missing | parent added   | do nothing
     *   missing  | older   | parent deleted | do nothing
     *   missing  | newer   | child added    | add to parent
     *
     * @throws Exception
     */
    private void addAndVerifySplitableStatements() throws Exception {
        // Old=older, New=newer, Mis=missing, P=parent, C=child, delP=del from parent, addP=add to parent, Noth=do nothing
        final RyaStatement stmtOldP_MisC_delP_a = createRyaStatementUniqueAdd("as_OldP_MisC", "ap_OldP_MisC", "ao_OldP_MisC", LAST_MONTH, parentDao, null);
        final RyaStatement stmtOldP_MisC_delP_f = createRyaStatementUniqueAdd("fs_OldP_MisC", "fp_OldP_MisC", "fo_OldP_MisC", LAST_MONTH, parentDao, null);
        final RyaStatement stmtOldP_MisC_delP_u = createRyaStatementUniqueAdd("us_OldP_MisC", "up_OldP_MisC", "uo_OldP_MisC", LAST_MONTH, parentDao, null);
        final RyaStatement stmtOldP_MisC_delP_z = createRyaStatementUniqueAdd("zs_OldP_MisC", "zp_OldP_MisC", "zo_OldP_MisC", LAST_MONTH, parentDao, null);
        final RyaStatement stmtNewP_MisC_Noth_a = createRyaStatementUniqueAdd("as_NewP_MisC", "ap_NewP_MisC", "ao_NewP_MisC", null      , parentDao, null);
        final RyaStatement stmtNewP_MisC_Noth_f = createRyaStatementUniqueAdd("fs_NewP_MisC", "fp_NewP_MisC", "fo_NewP_MisC", null      , parentDao, null);
        final RyaStatement stmtNewP_MisC_Noth_u = createRyaStatementUniqueAdd("us_NewP_MisC", "up_NewP_MisC", "uo_NewP_MisC", null      , parentDao, null);
        final RyaStatement stmtNewP_MisC_Noth_z = createRyaStatementUniqueAdd("zs_NewP_MisC", "zp_NewP_MisC", "zo_NewP_MisC", null      , parentDao, null);
        final RyaStatement stmtMisP_OldC_Noth_a = createRyaStatementUniqueAdd("as_MisP_OldC", "ap_MisP_OldC", "ao_MisP_OldC", LAST_MONTH, null     , childDao);
        final RyaStatement stmtMisP_OldC_Noth_f = createRyaStatementUniqueAdd("fs_MisP_OldC", "fp_MisP_OldC", "fo_MisP_OldC", LAST_MONTH, null     , childDao);
        final RyaStatement stmtMisP_OldC_Noth_u = createRyaStatementUniqueAdd("us_MisP_OldC", "up_MisP_OldC", "uo_MisP_OldC", LAST_MONTH, null     , childDao);
        final RyaStatement stmtMisP_OldC_addP_z = createRyaStatementUniqueAdd("zs_MisP_OldC", "zp_MisP_OldC", "zo_MisP_OldC", LAST_MONTH, null     , childDao);
        final RyaStatement stmtMisP_NewC_addP_a = createRyaStatementUniqueAdd("as_MisP_NewC", "ap_MisP_NewC", "ao_MisP_NewC", null      , null     , childDao);
        final RyaStatement stmtMisP_NewC_addP_f = createRyaStatementUniqueAdd("fs_MisP_NewC", "fp_MisP_NewC", "fo_MisP_NewC", null      , null     , childDao);
        final RyaStatement stmtMisP_NewC_addP_u = createRyaStatementUniqueAdd("us_MisP_NewC", "up_MisP_NewC", "uo_MisP_NewC", null      , null     , childDao);
        final RyaStatement stmtMisP_NewC_addP_z = createRyaStatementUniqueAdd("zs_MisP_NewC", "zp_MisP_NewC", "zo_MisP_NewC", null      , null     , childDao);

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        mergeToolRun(YESTERDAY);

        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        String desc = null;

        desc = "Old parent, missing Child, should delete Parent. ";
        assertStatementInParent(desc, 0, stmtOldP_MisC_delP_a);
        assertStatementInParent(desc, 0, stmtOldP_MisC_delP_f);
        assertStatementInParent(desc, 0, stmtOldP_MisC_delP_u);
        assertStatementInParent(desc, 0, stmtOldP_MisC_delP_z);

        desc = "New parent, missing Child, should do nothing, leave parent. ";
        assertStatementInParent(desc, 1, stmtNewP_MisC_Noth_a);
        assertStatementInParent(desc, 1, stmtNewP_MisC_Noth_f);
        assertStatementInParent(desc, 1, stmtNewP_MisC_Noth_u);
        assertStatementInParent(desc, 1, stmtNewP_MisC_Noth_z);

        desc = "Missing parent, Old Child, should do nothing, missing parent. ";
        assertStatementInParent(desc, 0, stmtMisP_OldC_Noth_a);
        assertStatementInParent(desc, 0, stmtMisP_OldC_Noth_f);
        assertStatementInParent(desc, 0, stmtMisP_OldC_Noth_u);
        assertStatementInParent(desc, 0, stmtMisP_OldC_addP_z);

        desc = "Missing parent, New Child, add to parent. ";
        assertStatementInParent(desc, 1, stmtMisP_NewC_addP_a);
        assertStatementInParent(desc, 1, stmtMisP_NewC_addP_f);
        assertStatementInParent(desc, 1, stmtMisP_NewC_addP_u);
        assertStatementInParent(desc, 1, stmtMisP_NewC_addP_z);
    }
}
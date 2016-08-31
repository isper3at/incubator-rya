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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.apache.rya.export.CopyType;
import org.apache.rya.export.DBType;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.accumulo.util.AccumuloRyaUtils;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.common.MergeProcessType;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.store.RyaStatementStore;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;

/**
 * Handles Merging a parent Accumulo instance with another DB child instance.
 */
public class AccumuloCopier implements Merger {
    private static final Logger log = Logger.getLogger(AccumuloCopier.class);

    private final AccumuloMergeConfiguration accumuloMergeConfiguration;
    private final AccumuloRyaStatementStore accumuloParentRyaStatementStore;
    private final RyaStatementStore childRyaStatementStore;
    private final AccumuloStatementManager accumuloStatementManager;

    private String startTime = null;
    private boolean useCopyFileOutput = false;
    private String baseOutputDir = null;
    private String localBaseOutputDir = null;
    private String compressionType = null;
    private boolean useCopyFileOutputDirectoryClear = false;
    private String tempDir = null;
    private boolean useCopyFileImport = false;
    private final boolean useQuery = false;
    private String localCopyFileImportDir = null;
    private String baseImportDir = null;

    private AccumuloInstanceDriver childAccumuloInstanceDriver = null;


    /**
     * Creates a new instance of {@link AccumuloCopier}.
     * @param accumuloMergeConfiguration the {@link AccumuloMergeConfiguration}.
     * (not {@code null})
     * @param accumuloParentRyaStatementStore the Accumulo parent
     * {@link RyaStatementStore}. (not {@code null})
     * @param childRyaStatementStore the child {@link RyaStatementStore}.
     * (not {@code null})
     * @param accumuloParentMetadataRepository the
     * {@link AccumuloParentMetadataRepository}. (not {@code null})
     */
    public AccumuloCopier(final AccumuloMergeConfiguration accumuloMergeConfiguration, final AccumuloRyaStatementStore accumuloParentRyaStatementStore, final RyaStatementStore childRyaStatementStore, final AccumuloParentMetadataRepository accumuloParentMetadataRepository) {
        this.accumuloMergeConfiguration = checkNotNull(accumuloMergeConfiguration);
        this.accumuloParentRyaStatementStore = checkNotNull(accumuloParentRyaStatementStore);
        this.childRyaStatementStore = checkNotNull(childRyaStatementStore);

        tempDir = accumuloParentRyaStatementStore.getRyaDAO().getConf().get("hadoop.tmp.dir", null);
        if (tempDir == null) {
            log.error("Invalid hadoop temp directory. \"hadoop.tmp.dir\" could not be found in the configuration.");
        }

        useCopyFileOutput = accumuloMergeConfiguration.getCopyType() == CopyType.FILE_EXPORT;
        useCopyFileImport = accumuloMergeConfiguration.getCopyType() == CopyType.FILE_IMPORT;
        baseOutputDir = tempDir + "/copy_tool_file_output/";
        localBaseOutputDir = accumuloMergeConfiguration.getOutputPath();
        compressionType = "gz";//conf.get(COPY_FILE_OUTPUT_COMPRESSION_TYPE, null);
        useCopyFileOutputDirectoryClear = true;//conf.getBoolean(USE_COPY_FILE_OUTPUT_DIRECTORY_CLEAR, false);
        localCopyFileImportDir = accumuloMergeConfiguration.getImportPath();
        baseImportDir = tempDir + "/copy_tool_import/";

        if (StringUtils.isNotBlank(compressionType)) {
            if (isValidCompressionType(compressionType)) {
                log.info("File compression type: " + compressionType);
            } else {
                log.warn("Invalid compression type: " + compressionType);
            }
        }

        startTime = accumuloMergeConfiguration.getToolStartTime();

        MergeParentMetadata mergeParentMetadata = null;
        try {
            mergeParentMetadata = accumuloParentMetadataRepository.get();
        } catch (final ParentMetadataDoesNotExistException e) {
            log.error("Error getting merge parent metadata from the repository.", e);
        }

        accumuloStatementManager = new AccumuloStatementManager(accumuloParentRyaStatementStore, childRyaStatementStore, mergeParentMetadata, MergeProcessType.COPY);
    }

    /**
     * @return the parent {@link AccumuloRyaStatementStore}.
     */
    public AccumuloRyaStatementStore getParentRyaStatementStore() {
        return accumuloParentRyaStatementStore;
    }

    /**
     * @return the child {@link RyaStatementStore}.
     */
    public RyaStatementStore getChildRyaStatementStore() {
       return childRyaStatementStore;
    }

    /**
     * @return the {@link AccumuloStatementManager}.
     */
    public AccumuloStatementManager getAccumuloStatementManager() {
        return accumuloStatementManager;
    }

    @Override
    public void runJob() {
        Instance childInstance = null;
        if (accumuloMergeConfiguration.getChildDBType() == DBType.ACCUMULO) {
            if (useCopyFileOutput) {
                fixSplitsInCachedLocalFiles();

                // Setup the offline child as a mini accumulo cluster
                try {
                    childInstance = createChildInstance(InstanceType.MINI);
                } catch (final Exception e) {
                    log.error("Error creating child instance", e);
                }
            } else if (useCopyFileImport) {
                try {
                    runImport();
                    return;
                } catch (final Exception e) {
                    log.error("Error importing data", e);
                }
            }
        }

        try {
            createTableIfNeeded(accumuloMergeConfiguration.getChildTablePrefix() + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);

            copyAuthorizations();
        } catch (final IOException e) {
            log.error("Unable to create tables", e);
        }

        try {
            // Go through the parent statements first.
            final Iterator<RyaStatement> parentRyaStatementIterator = accumuloParentRyaStatementStore.fetchStatements();

            RyaStatement parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            while (parentRyaStatement != null) {
                accumuloStatementManager.merge(Optional.fromNullable(parentRyaStatement), Optional.absent());

                parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            }
        } catch (final MergerException e) {
            log.error("Error encountered while merging", e);
        }

        if (useCopyFileOutput && childInstance != null) {
            Connector childConnector = null;
            try {
                childConnector = childInstance.getConnector(accumuloMergeConfiguration.getChildUsername(), new PasswordToken(accumuloMergeConfiguration.getChildPassword()));
            } catch (final AccumuloException | AccumuloSecurityException e) {
                log.error("Unable to get connector", e);
            }

            final TableOperations tableOperations = childConnector.tableOperations();
            for (final String tableName : tableOperations.list()) {
                try {
                    tableOperations.offline(tableName);
                    tableOperations.exportTable(tableName, localBaseOutputDir);
                } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                    log.error("Error exporting table", e);
                }
            }
        }
    }

    private static RyaStatement nextRyaStatement(final Iterator<RyaStatement> iterator) {
        RyaStatement ryaStatement = null;
        if (iterator.hasNext()) {
            ryaStatement = iterator.next();
        }
        return ryaStatement;
    }

    private void runImport() throws Exception {
        log.info("Setting up Copy Tool for importing...");

        createChildInstance(accumuloMergeConfiguration.getChildInstanceType());

        final File importDir = new File(localCopyFileImportDir);
        final String[] files = importDir.list();
        final List<String> tables = new ArrayList<>();
        tables.addAll(Arrays.asList(files));

        for (final String childTable : tables) {
            final String jobName = "Copy Tool, importing Exported Parent Table files from: " + getPath(localCopyFileImportDir, childTable).toString() + ", into Child Table: " + childTable + ", " + System.currentTimeMillis();
            log.info("Initializing job: " + jobName);

            final Date beginTime = new Date();
            log.info("Job for table \"" + childTable + "\" started: " + beginTime);

            createTableIfNeeded(childTable);
            importFilesToChildTable(childTable, childRyaStatementStore.getRyaDAO().getConf());

            final Date endTime = new Date();
            log.info("Job for table \"" + childTable + "\" finished: " + endTime);
            log.info("The job took " + (endTime.getTime() - beginTime.getTime()) / 1000 + " seconds.");
        }
    }

    /**
     * Creates the child table if it doesn't already exist.
     * @param childTableName the name of the child table.
     * @throws IOException
     */
    public void createTableIfNeeded(final String childTableName) throws IOException {
        try {
            final Configuration childConfig = childRyaStatementStore.getRyaDAO().getConf();
            final AccumuloRdfConfiguration childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(childConfig);
            childAccumuloRdfConfiguration.setTablePrefix(accumuloMergeConfiguration.getChildTablePrefix());
            final Connector childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
            if (!childConnector.tableOperations().exists(childTableName)) {
                log.info("Creating table: " + childTableName);
                childConnector.tableOperations().create(childTableName);
                log.info("Created table: " + childTableName);
                log.info("Granting authorizations to table: " + childTableName);
                childConnector.securityOperations().grantTablePermission(accumuloMergeConfiguration.getChildUsername(), childTableName, TablePermission.WRITE);
                log.info("Granted authorizations to table: " + childTableName);
            }
        } catch (TableExistsException | AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    private void setupSplitsFile(/*final Job job, */final TableOperations parentTableOperations, final String parentTableName, final String childTableName) throws Exception {
        final FileSystem fs = FileSystem.get(accumuloParentRyaStatementStore.getRyaDAO().getConf());
        fs.setPermission(getPath(baseOutputDir, childTableName), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        final Path splitsPath = getPath(baseOutputDir, childTableName, "splits.txt");
        final Collection<Text> splits = parentTableOperations.listSplits(parentTableName, 100);
        log.info("Creating splits file at: " + splitsPath);
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {
            for (final Text split : splits) {
                final String encoded = new String(Base64.encodeBase64(TextUtil.getBytes(split)));
                out.println(encoded);
            }
        }
        fs.setPermission(splitsPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        final String userDir = System.getProperty("user.dir");
        // The splits file has a symlink created in the user directory for some reason.
        // It might be better to copy the entire file for Windows but it doesn't seem to matter if
        // the user directory symlink is broken.
        java.nio.file.Files.deleteIfExists(new File(userDir, "splits.txt").toPath());
        //Files.copy(new File(splitsPath.toString()), new File(userDir, "splits.txt"));
//        job.setPartitionerClass(KeyRangePartitioner.class);
//        KeyRangePartitioner.setSplitFile(job, splitsPath.toString());
//        job.setNumReduceTasks(splits.size() + 1);
    }

    /**
     * Converts a path string, or a sequence of strings that when joined form a path string,
     * to a {@link org.apache.hadoop.fs.Path}.
     * @param first The path string or initial part of the path string.
     * @param more Additional strings to be joined to form the path string.
     * @return the resulting {@link org.apache.hadoop.fs.Path}.
     */
    public static Path getPath(final String first, final String... more) {
        final java.nio.file.Path path = Paths.get(first, more);
        final String stringPath = FilenameUtils.separatorsToUnix(path.toAbsolutePath().toString());
        final Path hadoopPath = new Path(stringPath);
        return hadoopPath;
    }

    /**
     * Imports the files that hold the table data into the child instance.
     * @param childTableName the name of the child table to import.
     * @param conf the {@link Configuration}.
     * @throws Exception
     */
    public void importFilesToChildTable(final String childTableName, final Configuration conf) throws Exception {
        final AccumuloRdfConfiguration childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(conf);
        childAccumuloRdfConfiguration.setTablePrefix(accumuloMergeConfiguration.getChildTablePrefix());
        final Connector childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
        final TableOperations childTableOperations = childConnector.tableOperations();

        final Path localWorkDir = getPath(localCopyFileImportDir, childTableName);
        final Path hdfsBaseWorkDir = getPath(baseImportDir, childTableName);

        copyLocalToHdfs(localWorkDir, hdfsBaseWorkDir, conf);

        final Path files = getPath(hdfsBaseWorkDir.toString(), "files");
        final Path failures = getPath(hdfsBaseWorkDir.toString(), "failures");
        final FileSystem fs = FileSystem.get(conf);
        // With HDFS permissions on, we need to make sure the Accumulo user can read/move the files
        fs.setPermission(hdfsBaseWorkDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        if (fs.exists(failures)) {
            fs.delete(failures, true);
        }
        fs.mkdirs(failures);

        childTableOperations.importDirectory(childTableName, files.toString(), failures.toString(), false);
    }

    /**
     * Copies the file from the local file system into the HDFS.
     * @param localInputPath the local system input {@link Path}.
     * @param hdfsOutputPath the HDFS output {@link Path}.
     * @param configuration the {@link Configuration} to use.
     * @throws IOException
     */
    public static void copyLocalToHdfs(final Path localInputPath, final Path hdfsOutputPath, final Configuration configuration) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        fs.copyFromLocalFile(localInputPath, hdfsOutputPath);
    }

    /**
     * Copies the file from HDFS into the local file system.
     * @param hdfsInputPath the HDFS input {@link Path}.
     * @param localOutputPath the local system output {@link Path}.
     * @param configuration the {@link Configuration} to use.
     * @throws IOException
     */
    public static void copyHdfsToLocal(final Path hdfsInputPath, final Path localOutputPath, final Configuration configuration) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        fs.copyToLocalFile(hdfsInputPath, localOutputPath);
    }

    /**
     * Fixes the "splits.txt" file path in the "mapreduce.job.cache.local.files" property.  It contains the
     * {@link URI} "file:" prefix which causes {@link KeyRangePartitioner} to throw a {@code FileNotFoundException}
     * when it attempts to open it.
     */
    private void fixSplitsInCachedLocalFiles() {
        if (useCopyFileOutput) {
            // The "mapreduce.job.cache.local.files" property contains a comma-separated
            // list of cached local file paths.
            final String cachedLocalFiles = accumuloParentRyaStatementStore.getRyaDAO().getConf().get(MRJobConfig.CACHE_LOCALFILES);
            if (cachedLocalFiles != null) {
                final List<String> cachedLocalFilesList = Lists.newArrayList(Splitter.on(',').split(cachedLocalFiles));
                final List<String> formattedCachedLocalFilesList = new ArrayList<>();
                for (final String cachedLocalFile : cachedLocalFilesList) {
                    String pathToAdd = cachedLocalFile;
                    if (cachedLocalFile.endsWith("splits.txt")) {
                        URI uri = null;
                        try {
                            uri = new URI(cachedLocalFiles);
                            pathToAdd = uri.getPath();
                        } catch (final URISyntaxException e) {
                            log.error("Invalid syntax in local cache file path", e);
                        }
                    }
                    formattedCachedLocalFilesList.add(pathToAdd);
                }
                final String formattedCachedLocalFiles = Joiner.on(',').join(formattedCachedLocalFilesList);
                if (!cachedLocalFiles.equals(formattedCachedLocalFiles)) {
                    accumuloParentRyaStatementStore.getRyaDAO().getConf().set(MRJobConfig.CACHE_LOCALFILES, formattedCachedLocalFiles);
                }
            }
        }
    }

    /**
     * Checks to see if the specified compression type is valid. The compression must be defined in
     * {@link Algorithm} to be valid.
     * @param compressionType the compression type to check.
     * @return {@code true} if the compression type is one of "none", "gz", "lzo", or "snappy".
     * {@code false} otherwise.
     */
    private static boolean isValidCompressionType(final String compressionType) {
        for (final Algorithm algorithm : Algorithm.values()) {
            if (algorithm.getName().equals(compressionType)) {
                return true;
            }
        }
        return false;
    }

    private void clearOutputDir(final Path path, final Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }

    private Instance createChildInstance(final Configuration config) throws Exception {
        final Instance instance = null;
        String instanceTypeProp = config.get(AccumuloExportConstants.CREATE_CHILD_INSTANCE_TYPE_PROP);
        final String childAuth = config.get(MRUtils.AC_AUTH_PROP + AccumuloExportConstants.CHILD_SUFFIX);

        // Default to distribution cluster if not specified
        if (StringUtils.isBlank(instanceTypeProp)) {
            instanceTypeProp = InstanceType.DISTRIBUTION.toString();
        }

        final InstanceType instanceType = InstanceType.fromName(instanceTypeProp);

        return createChildInstance(instanceType);
    }

    private Instance createChildInstance(final InstanceType instanceType) throws Exception {
        Instance instance = null;

        switch (instanceType) {
            case DISTRIBUTION:
                if (accumuloMergeConfiguration.getChildRyaInstanceName() == null) {
                    throw new IllegalArgumentException("Must specify instance name for distributed mode");
                } else if (accumuloMergeConfiguration.getChildZookeepers() == null) {
                    throw new IllegalArgumentException("Must specify ZooKeeper hosts for distributed mode");
                }
                instance = new ZooKeeperInstance(accumuloMergeConfiguration.getChildRyaInstanceName(), accumuloMergeConfiguration.getChildZookeepers());
                break;

            case MINI:
                childAccumuloInstanceDriver = new AccumuloInstanceDriver("Child", accumuloMergeConfiguration.getChildInstanceType(), true, false, false, accumuloMergeConfiguration.getChildUsername(), accumuloMergeConfiguration.getChildPassword(), accumuloMergeConfiguration.getChildRyaInstanceName(), accumuloMergeConfiguration.getChildTablePrefix(), accumuloMergeConfiguration.getChildAuths());
                childAccumuloInstanceDriver.setUpInstance();
                childAccumuloInstanceDriver.setUpTables();
                final String childZk = childAccumuloInstanceDriver.getZooKeepers();
                instance = new ZooKeeperInstance(accumuloMergeConfiguration.getChildRyaInstanceName(), childZk);
                break;

            case MOCK:
                instance = new MockInstance(accumuloMergeConfiguration.getChildRyaInstanceName());
                break;

            default:
                throw new AccumuloException("Unexpected instance type: " + instanceType);
        }

        return instance;
    }

    protected void copyAuthorizations() throws IOException {
        try {
            final String parentUser = accumuloMergeConfiguration.getParentUsername();
            final String childUser = accumuloMergeConfiguration.getChildUsername();
            final Connector parentConnector = accumuloParentRyaStatementStore.getRyaDAO().getConnector();
            final Connector childConnector = ((AccumuloRyaStatementStore) childRyaStatementStore).getRyaDAO().getConnector();
            final SecurityOperations parentSecOps = parentConnector.securityOperations();
            final SecurityOperations childSecOps = childConnector.securityOperations();

            final Authorizations parentAuths = parentSecOps.getUserAuthorizations(parentUser);
            final Authorizations childAuths = childSecOps.getUserAuthorizations(childUser);
            // Add any parent authorizations that the child doesn't have.
            if (!childAuths.equals(parentAuths)) {
                log.info("Adding the authorization, \"" + parentAuths.toString() + "\", to the child user, \"" + childUser + "\"");
                final Authorizations newChildAuths = AccumuloRyaUtils.addUserAuths(childUser, childSecOps, parentAuths);
                childSecOps.changeUserAuthorizations(childUser, newChildAuths);
            }
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

}
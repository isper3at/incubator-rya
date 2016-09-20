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
package org.apache.rya.export.client.merge;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.StatementMerger;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.parent.ParentMetadataExistsException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;

import mvm.rya.api.domain.RyaStatement;

/**
 * An in memory {@link Merger}.  Merges {@link RyaStatement}s from a parent
 * to a child.  The statements merged will be any that have a timestamp after
 * the provided time.  If there are any conflicting statements, the provided
 * {@link StatementMerger} will merge the statements and produce the desired
 * {@link RyaStatement}.
 */
public class MemoryTimeMerger implements Merger {
    private static final Logger LOG = Logger.getLogger(MemoryTimeMerger.class);

    private final RyaStatementStore parentStore;
    private final ParentMetadataRepository parentMetadata;
    private final RyaStatementStore childStore;
    private final ParentMetadataRepository childMetadata;
    private final StatementMerger statementMerger;
    private final Date timestamp;
    private final String ryaInstanceName;
    private final Long timeOffset;

    /**
     * Creates a new {@link MemoryTimeMerger} to merge the statements from the parent to a child.
     * @param parentStore
     * @param childStore
     * @param childMetadata
     * @param parentMetadata
     * @param statementMerger
     * @param timestamp - The timestamp from which all parent statements will be merged into the child.
     */
    public MemoryTimeMerger(final RyaStatementStore parentStore, final RyaStatementStore childStore,
            final ParentMetadataRepository parentMetadata, final ParentMetadataRepository childMetadata,
            final StatementMerger statementMerger, final Date timestamp, final String ryaInstanceName,
            final Long timeOffset) {
        this.parentStore = checkNotNull(parentStore);
        this.parentMetadata = checkNotNull(parentMetadata);
        this.childStore = checkNotNull(childStore);
        this.childMetadata = checkNotNull(childMetadata);
        this.statementMerger = checkNotNull(statementMerger);
        this.timestamp = checkNotNull(timestamp);
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
        this.timeOffset = checkNotNull(timeOffset);
    }

    @Override
    public void runJob() {
        LOG.info("Merging statements...");
        MergeParentMetadata metadata = null;
        try {
            metadata = parentMetadata.get();
        } catch (final ParentMetadataDoesNotExistException e) {
            LOG.debug("Parent has no metadata on the child.", e);
        }

        //check the parent for a parent metadata repo
        if(metadata != null) {
            if(metadata.getRyaInstanceName().equals(ryaInstanceName)) {
                try {
                    importStatements();
                } catch (AddStatementException | ContainsStatementException | RemoveStatementException | FetchStatementException e) {
                    LOG.error("Failed to import statements.", e);
                }
            }
        } else {
            try {
                export();
            } catch (final ParentMetadataExistsException | FetchStatementException e) {
                LOG.error("Failed to export statements.", e);
            }
        }
    }

    /**
     * Exports all statements after the provided timestamp.
     * @throws ParentMetadataExistsException -
     * @throws FetchStatementException
     */
    private void export() throws ParentMetadataExistsException, FetchStatementException {
        LOG.info("Creating parent metadata in the child.");
        //setup parent metadata repo in the child
        final MergeParentMetadata metadata = new MergeParentMetadata.Builder()
            .setRyaInstanceName(ryaInstanceName)
            .setTimestamp(timestamp)
            .setParentTimeOffset(timeOffset)
            .setFilterTimestmap(timestamp)
            .build();
        childMetadata.set(metadata);

        //fetch all statements after timestamp from the parent
        final Iterator<RyaStatement> statements = parentStore.fetchStatements();
        LOG.info("Exporting statements.");
        while(statements.hasNext()) {
            final RyaStatement statement = statements.next();
            System.out.println(statement.toString());
            try {
                childStore.addStatement(statement);
            } catch (final AddStatementException e) {
                LOG.error("Failed to add statement: " + statement + " to the statement store.", e);
            }
        }
    }

    private void importStatements() throws AddStatementException, ContainsStatementException, RemoveStatementException, FetchStatementException {
        LOG.info("Importing statements.");
        final Iterator<RyaStatement> parentStatements = parentStore.fetchStatements();
        final Iterator<RyaStatement> childStatements = childStore.fetchStatements();
        //statements are in order by timestamp.

        long curTime = -1L;
        //Remove statements that were removed in the child.
        //after the timestamp has passed, there is no need to keep checking the parent
        while(childStatements.hasNext() && curTime < timestamp.getTime()) {
            final RyaStatement statement = childStatements.next();
            if(!parentStore.containsStatement(statement)) {
                childStore.removeStatement(statement);
            }
        }

        //Add all of the child statements that are not in the parent
        while(parentStatements.hasNext()) {
            final RyaStatement statement = parentStatements.next();
            curTime = statement.getTimestamp();
            if(!childStore.containsStatement(statement)) {
                statement.setTimestamp(statement.getTimestamp() - timeOffset);
                childStore.addStatement(statement);
            }
        }
    }
}

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
package org.apache.rya.indexing.pcj.storage;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;

public abstract class AbstractPcjStorage implements PrecomputedJoinStorage {
    // Used to update the instance's metadata.
    protected final RyaDetailsRepository ryaDetailsRepo;

    protected final String ryaInstanceName;

    // Factories that are used to create new PCJs.
    protected final PCJIdFactory pcjIdFactory = new PCJIdFactory();
    protected final PcjTableNameFactory pcjTableNameFactory = new PcjTableNameFactory();
    protected final PcjVarOrderFactory pcjVarOrderFactory = new ShiftVarOrderFactory();

    public AbstractPcjStorage(final RyaDetailsRepository ryaDetailsRepo, final String ryaInstanceName) {
        this.ryaDetailsRepo = requireNonNull(ryaDetailsRepo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
    }

    @Override
    public List<String> listPcjs() throws PCJStorageException {
        try {
            final RyaDetails details = ryaDetailsRepo.getRyaInstanceDetails();
            final PCJIndexDetails pcjIndexDetails = details.getPCJIndexDetails();
            final List<String> pcjIds = new ArrayList<>(pcjIndexDetails.getPCJDetails().keySet());
            return pcjIds;
        } catch (final RyaDetailsRepositoryException e) {
            throw new PCJStorageException("Could not check to see if RyaDetails exist for the instance.", e);
        }
    }

    protected void addPcj(final String pcjId) throws PCJStorageException {
        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(originalDetails -> {
                // Create the new PCJ's details.
                final PCJDetails.Builder newPcjDetails = PCJDetails.builder().setId(pcjId);

                // Add them to the instance's details.
                final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                mutated.getPCJIndexDetails().addPCJDetails(newPcjDetails);
                return mutated.build();
            });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not create a new PCJ for Rya instance '%s' "
                    + "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }
    }
}

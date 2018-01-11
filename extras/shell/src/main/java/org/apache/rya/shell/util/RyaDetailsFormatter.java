/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.shell.util;

import static java.util.Objects.requireNonNull;

import java.util.Set;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails.TypeDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.shell.SharedShellState.StorageType;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Formats an instance of {@link RyaDetails}.
 */
@DefaultAnnotation(NonNull.class)
public class RyaDetailsFormatter {

    /**
     * Pretty formats an instance of {@link RyaDetails}.
     *
     * @param storageType - The type of storage the instance is installed on. (not null)
     * @param details - The object to format. (not null)
     * @return A pretty render of the object.
     */
    public String format(final StorageType storageType, final RyaDetails details) {
        requireNonNull(details);

        final StringBuilder report = new StringBuilder();

        report.append("General Metadata:\n");
        report.append("  Instance Name: ").append(details.getRyaInstanceName()).append("\n");
        report.append("  RYA Version: ").append( details.getRyaVersion() ).append("\n");

        if(storageType == StorageType.ACCUMULO) {
            report.append("  Users: ").append( Joiner.on(", ").join(details.getUsers()) ).append("\n");
        }

        report.append("Secondary Indicies:\n");

        final EntityCentricIndexDetails entityDetails = details.getEntityCentricIndexDetails();
        report.append("  Entity Centric Index:\n");
        report.append("    Enabled: ").append( entityDetails.isEnabled() ).append("\n");
        if(storageType == StorageType.MONGO) {
            report.append("  Types:\n");
            final Set<TypeDetails> types = entityDetails.getTypes();
            if(types.size() == 0) {
                report.append("    No Types have been added yet.\n");
            } else {
                for(final TypeDetails type : types) {
                    report.append("      ID: ").append(type.getId().getData()).append("\n");

                    final ImmutableSet<RyaURI> props = type.getProperties();
                    if(props.size() == 0) {
                        //this should never happen, a property-less type is invalid.
                        report.append("      No properties have been added.\n");
                    } else {
                        report.append("      Properties: \n");
                        for(final RyaURI prop : props) {
                            report.append("        ").append(prop.getData()).append("\n");
                        }
                    }
                }
            }
        }

      //RYA-215        report.append("  Geospatial Index:\n");
      //RYA-215        report.append("    Enabled: ").append( details.getGeoIndexDetails().isEnabled() ).append("\n");
        report.append("  Free Text Index:\n");
        report.append("    Enabled: ").append( details.getFreeTextIndexDetails().isEnabled() ).append("\n");
        report.append("  Temporal Index:\n");
        report.append("    Enabled: ").append( details.getTemporalIndexDetails().isEnabled() ).append("\n");

        report.append("  PCJ Index:\n");
        final PCJIndexDetails pcjDetails = details.getPCJIndexDetails();
        report.append("    Enabled: ").append( pcjDetails.isEnabled() ).append("\n");
        if(pcjDetails.isEnabled()) {
            if(pcjDetails.getFluoDetails().isPresent()) {
                final String fluoAppName = pcjDetails.getFluoDetails().get().getUpdateAppName();
                report.append("    Fluo App Name: ").append(fluoAppName).append("\n");
            }

            final ImmutableMap<String, PCJDetails> pcjs = pcjDetails.getPCJDetails();
            report.append("    PCJs:\n");
            if(pcjs.isEmpty()) {
                report.append("      No PCJs have been added yet.\n");
            } else {
                for(final PCJDetails pcj : pcjs.values()) {
                    report.append("      ID: ").append(pcj.getId()).append("\n");

                    final String updateStrategy = format( pcj.getUpdateStrategy(), "None" );
                    report.append("        Update Strategy: ").append(updateStrategy).append("\n");

                    final String lastUpdateTime = format( pcj.getLastUpdateTime(), "unavailable");
                    report.append("        Last Update Time: ").append(lastUpdateTime).append("\n");
                }
            }

            if (storageType == StorageType.ACCUMULO) {
                report.append("Statistics:\n");
                report.append("  Prospector:\n");
                final String prospectorLastUpdateTime = format(details.getProspectorDetails().getLastUpdated(),
                        "unavailable");
                report.append("    Last Update Time: ").append(prospectorLastUpdateTime).append("\n");

                report.append("  Join Selectivity:\n");
                final String jsLastUpdateTime = format(details.getJoinSelectivityDetails().getLastUpdated(),
                        "unavailable");
                report.append("    Last Updated Time: ").append(jsLastUpdateTime).append("\n");
            }
        }

        return report.toString();
    }

    /**
     * Formats an optional value using the value's toString() value.
     *
     * @param value - The optional value that will be formatted. (not null)
     * @param absentValue - The String that will be returned if the optional is absent. (not null)
     * @return The String representation of the optional value.
     */
    private <T> String format(final Optional<T> value, final String absentValue) {
        requireNonNull(value);

        String formatted = "unavailable";
        if(value.isPresent()) {
            formatted = value.get().toString();
        }
        return formatted;
    }
}
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
package org.apache.rya.api.client;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.rya.api.domain.RyaURI;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

/**
 * Adds a Type for the Entity Indexer to use.
 */
@DefaultAnnotation(NonNull.class)
public interface AddEntityType {

    /**
     * Adds an Entity Type for the Entity Indexer to use.
     *
     * @param instanceName - Indicates which Rya instance the type will be added to. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void addEntityType(final String ryaInstanceName, final EntityTypeConfiguration typeConfiguration) throws InstanceDoesNotExistException, RyaClientException;

    /**
     * Configures how an Entity Indexer Type will be configured.
     */
    @Immutable
    @DefaultAnnotation(NonNull.class)
    public static class EntityTypeConfiguration {

        private final RyaURI typeID;
        private final ImmutableSet<RyaURI> typeProperties;

        /**
         * Use a {@link Builder} to create instances of this class.
         */
        private EntityTypeConfiguration(
                final RyaURI typeID,
                final ImmutableSet<RyaURI> typeProperties) {
            this.typeID = requireNonNull(typeID);
            this.typeProperties = requireNonNull(typeProperties);
        }

        /**
         * @return The {@link RyaURI} to be the ID for this Type.
         */
        public RyaURI getId() {
            return typeID;
        }

        /**
         * @return The {@link RyaURI} properties that define this Type.
         */
        public ImmutableSet<RyaURI> getProperties() {
            return typeProperties;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    typeID,
                    typeProperties);
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof EntityTypeConfiguration) {
                final EntityTypeConfiguration config = (EntityTypeConfiguration) obj;
                return Objects.equals(typeID, config.typeID) &&
                       Objects.equals(typeProperties, config.typeProperties);
            }
            return false;
        }

        /**
         * @return An empty instance of {@link Builder}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builds instances of {@link EntityTypeConfiguration}.
         */
        @DefaultAnnotation(NonNull.class)
        public static class Builder {
            private RyaURI typeID = null;
            private ImmutableSet.Builder<RyaURI> propertyBuilder = null;

            /**
             * @param typeId - The {@link RyaURI} to use as an Id for this Type.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setTypeId(final RyaURI typeId) {
                typeID = requireNonNull(typeId);
                return this;
            }

            /**
             * @param property - A {@link RyaURI} to use as a property to define this Type.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder addProperty(final RyaURI property) {
                if(propertyBuilder == null) {
                    propertyBuilder = new ImmutableSet.Builder<>();
                }
                propertyBuilder.add(property);
                return this;
            }

            /**
             * @return Builds an instance of {@link EntityTypeConfiguration} using this builder's values.
             */
            public EntityTypeConfiguration build() {
                Preconditions.checkState(propertyBuilder != null, "No properties have been defined for this Type.");
                return new EntityTypeConfiguration(
                        typeID,
                        propertyBuilder.build());
            }
        }
    }
}
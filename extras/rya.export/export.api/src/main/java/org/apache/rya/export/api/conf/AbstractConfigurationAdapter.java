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
package org.apache.rya.export.api.conf;

import org.apache.rya.export.JAXBMergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration.Builder;

/**
 * Abstract configuration adapter for creating a merge configuration object.
 *
 * @param <B> the extension of {@link Builder}.
 * @param <C> the extension of {@link JAXBMergeConfiguration}.
 */
public abstract class AbstractConfigurationAdapter<B extends Builder, C extends JAXBMergeConfiguration> {
    /**
     * Makes a builder that has its properties populated by the
     * specified configuration.
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link Builder} used in the application
     * @throws MergeConfigurationException
     */
    public abstract B makeBuilder(final C jConfig) throws MergeConfigurationException;


    /**
     * Creates a new empty instance of the {@link Builder}.
     * @return the newly instantiated {@link Builder} instance.
     */
    public abstract B getBuilderInstance();

    /**
     * Populates the builder's properties from the specified configuration.
     * @param builder - The config object Builder.
     * @param jConfig - The JAXB generated configuration.
     * @throws MergeConfigurationException
     */
    public abstract void setBuilderParams(final B builder, final C jConfig) throws MergeConfigurationException;

    /**
     * Constructs a {@link MergeConfiguration} from the supplied JAXB generated
     * configuration.
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    public abstract MergeConfiguration createConfig(final C jConfig) throws MergeConfigurationException;
}

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

import org.apache.rya.export.JAXBAccumuloMergeConfiguration;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration.AccumuloBuilder;

/**
 * Helper for creating the immutable application configuration that uses
 * Accumulo.
 */
public class AccumuloConfigurationAdapter extends ConfigurationAdapter<AccumuloBuilder, JAXBAccumuloMergeConfiguration> {
    @Override
    public AccumuloBuilder makeBuilder(final JAXBAccumuloMergeConfiguration jConfig) throws MergeConfigurationException {
        final AccumuloBuilder configBuilder = new AccumuloBuilder();
        setBuilderParams(configBuilder, jConfig);
        return configBuilder;
    }

    @Override
    public void setBuilderParams(final AccumuloBuilder builder, final JAXBAccumuloMergeConfiguration jAccumuloConfig) {
        super.setBuilderParams(builder, jAccumuloConfig);

        builder
            .setParentZookeepers(jAccumuloConfig.getParentZookeepers())
            .setParentAuths(jAccumuloConfig.getParentAuths())
            .setParentInstanceType(InstanceType.fromName(jAccumuloConfig.getParentInstanceType()))
            .setChildZookeepers(jAccumuloConfig.getChildZookeepers())
            .setChildAuths(jAccumuloConfig.getChildAuths())
            .setChildInstanceType(InstanceType.fromName(jAccumuloConfig.getChildInstanceType()));
    }

    @Override
    public AccumuloMergeConfiguration createConfig(final JAXBAccumuloMergeConfiguration jConfig) throws MergeConfigurationException {
        final AccumuloBuilder configBuilder = makeBuilder(jConfig);
        return configBuilder.build();
    }
}

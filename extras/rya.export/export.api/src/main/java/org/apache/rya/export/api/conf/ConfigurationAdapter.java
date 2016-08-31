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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

import org.apache.rya.export.JAXBMergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration.Builder;

/**
 * Helper for creating the immutable application configuration.
 * @param <B> the type of {@link Builder} to use for the adapter.
 * @param <C> the type of {@link JAXBMergeConfiguration} to use for the adapter.
 */
public class ConfigurationAdapter<B extends Builder, C extends JAXBMergeConfiguration> extends AbstractConfigurationAdapter<B, C> {
    @Override
    public B makeBuilder(final C jConfig) throws MergeConfigurationException {
        final B builder = getBuilderInstance();
        setBuilderParams(builder, jConfig);
        return builder;
    }

    @Override
    public B getBuilderInstance() {
        final B builder = createTemplateInstance(this, 0);
        return builder;
    }

    /**
     * @param object instance of a class that is a subclass of a generic class
     * @param index index of the generic type that should be instantiated
     * @return new instance of T (created by calling the default constructor)
     * @throws RuntimeException if T has no accessible default constructor
     */
    @SuppressWarnings("unchecked")
    public static <T> T createTemplateInstance(final Object object, final int index) {
        final TypeVariable<?>[] typeVariables = object.getClass().getTypeParameters();
        final TypeVariable<?> typeVariable = typeVariables[index];
        final Type type = typeVariable.getAnnotatedBounds()[index].getType();
        Class<T> instanceType = null;
        if (type instanceof ParameterizedType) {
            final ParameterizedType paramType = (ParameterizedType) type;
            instanceType = (Class<T>) paramType.getActualTypeArguments()[0];
        } else if (type instanceof Class) {
            instanceType = (Class<T>) type;
        }

        try {
          return instanceType.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setBuilderParams(final B builder, final C jConfig) {
        builder
            .setParentHostname(jConfig.getParentHostname())
            .setParentUsername(jConfig.getParentUsername())
            .setParentPassword(jConfig.getParentPassword())
            .setParentRyaInstanceName(jConfig.getParentRyaInstanceName())
            .setParentTablePrefix(jConfig.getParentTablePrefix())
            .setParentTomcatUrl(jConfig.getParentTomcalUrl())
            .setParentDBType(jConfig.getParentDBType())
            .setParentPort(jConfig.getParentPort())
            .setChildHostname(jConfig.getChildHostname())
            .setChildUsername(jConfig.getChildUsername())
            .setChildPassword(jConfig.getChildPassword())
            .setChildRyaInstanceName(jConfig.getChildRyaInstanceName())
            .setChildTablePrefix(jConfig.getChildTablePrefix())
            .setChildTomcatUrl(jConfig.getChildTomcalUrl())
            .setChildDBType(jConfig.getChildDBType())
            .setChildPort(jConfig.getChildPort())
            .setMergePolicy(jConfig.getMergePolicy())
            .setCopyType(jConfig.getCopyType())
            .setOutputPath(jConfig.getOutputPath())
            .setImportPath(jConfig.getImportPath())
            .setUseNtpServer(jConfig.isUseNtpServer())
            .setNtpServerHost(jConfig.getNtpServerHost())
            .setToolStartTime(jConfig.getToolStartTime());
    }

    @Override
    public MergeConfiguration createConfig(final C jConfig) throws MergeConfigurationException {
        final B configBuilder = makeBuilder(jConfig);
        return configBuilder.build();
    }
}

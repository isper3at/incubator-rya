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

import static java.util.Objects.requireNonNull;

import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;

/**
 *
 */
public class MergeConfigurationDecorator extends MergeConfiguration {

    private final MergeConfiguration conf;

    public MergeConfigurationDecorator(final MergeConfiguration conf, final MergeConfiguration.Builder builder) throws MergeConfigurationException {
        super(builder);
        this.conf = requireNonNull(conf);
    }

    @Override
    public int hashCode() {
        return conf.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return conf.equals(obj);
    }

    @Override
    public String getParentHostname() {
        return conf.getParentHostname();
    }

    @Override
    public String getParentUsername() {
        return conf.getParentUsername();
    }

    @Override
    public String getParentPassword() {
        return conf.getParentPassword();
    }

    @Override
    public String getParentRyaInstanceName() {
        return conf.getParentRyaInstanceName();
    }

    @Override
    public String getParentTablePrefix() {
        return conf.getParentTablePrefix();
    }

    @Override
    public String getParentTomcatUrl() {
        return conf.getParentTomcatUrl();
    }

    @Override
    public DBType getParentDBType() {
        return conf.getParentDBType();
    }

    @Override
    public int getParentPort() {
        return conf.getParentPort();
    }

    @Override
    public String getChildHostname() {
        return conf.getChildHostname();
    }

    @Override
    public String getChildUsername() {
        return conf.getChildUsername();
    }

    @Override
    public String getChildPassword() {
        return conf.getChildPassword();
    }

    @Override
    public String getChildRyaInstanceName() {
        return conf.getChildRyaInstanceName();
    }

    @Override
    public String getChildTablePrefix() {
        return conf.getChildTablePrefix();
    }

    @Override
    public String getChildTomcatUrl() {
        return conf.getChildTomcatUrl();
    }

    @Override
    public DBType getChildDBType() {
        return conf.getChildDBType();
    }

    @Override
    public int getChildPort() {
        return conf.getChildPort();
    }

    @Override
    public MergePolicy getMergePolicy() {
        return conf.getMergePolicy();
    }

    @Override
    public Boolean getUseNtpServer() {
        return conf.getUseNtpServer();
    }

    @Override
    public String getNtpServerHost() {
        return conf.getNtpServerHost();
    }

    @Override
    public String getToolStartTime() {
        return conf.getToolStartTime();
    }

    @Override
    public String toString() {
        return conf.toString();
    }
}

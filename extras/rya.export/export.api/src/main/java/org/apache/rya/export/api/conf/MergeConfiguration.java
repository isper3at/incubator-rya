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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.http.annotation.Immutable;
import org.apache.rya.export.CopyType;
import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;

/**
 * Immutable configuration object to allow the MergeTool to connect to the parent and child
 * databases for data merging.
 */
@Immutable
public class MergeConfiguration {
    /**
     * Information needed to connect to the parent database
     */
    private final String parentHostname;
    private final String parentUsername;
    private final String parentPassword;
    private final String parentRyaInstanceName;
    private final String parentTablePrefix;
    private final String parentTomcatUrl;
    private final DBType parentDBType;
    private final int parentPort;

    /**
     * Information needed to connect to the child database
     */
    private final String childHostname;
    private final String childUsername;
    private final String childPassword;
    private final String childRyaInstanceName;
    private final String childTablePrefix;
    private final String childTomcatUrl;
    private final DBType childDBType;
    private final int childPort;

    private final MergePolicy mergePolicy;
    private final CopyType copyType;

    private final String outputPath;
    private final String importPath;
    private final boolean useNtpServer;
    private final String ntpServerHost;
    private final String toolStartTime;

    /**
     * Constructs a {@link MergeConfiguration}.
     */
    protected MergeConfiguration(final Builder builder) throws MergeConfigurationException {
        try {
            checkNotNull(builder);
        } catch(final NullPointerException npe) {
            throw new MergeConfigurationException("The configuration was passed a null builder.", npe);
        }
        try {
            this.parentHostname = checkNotNull(builder.parentHostname);
            this.parentUsername = checkNotNull(builder.parentUsername);
            this.parentPassword = checkNotNull(builder.parentPassword);
            this.parentRyaInstanceName = checkNotNull(builder.parentRyaInstanceName);
            this.parentTablePrefix = checkNotNull(builder.parentTablePrefix);
            this.parentTomcatUrl = checkNotNull(builder.parentTomcatUrl);
            this.parentDBType = checkNotNull(builder.parentDBType);
            this.parentPort = checkNotNull(builder.parentPort);
            this.childHostname = checkNotNull(builder.childHostname);
            this.childUsername = checkNotNull(builder.childUsername);
            this.childPassword = checkNotNull(builder.childPassword);
            this.childRyaInstanceName = checkNotNull(builder.childRyaInstanceName);
            this.childTablePrefix = checkNotNull(builder.childTablePrefix);
            this.childTomcatUrl = checkNotNull(builder.childTomcatUrl);
            this.childDBType = checkNotNull(builder.childDBType);
            this.childPort = checkNotNull(builder.childPort);
            this.mergePolicy = checkNotNull(builder.mergePolicy);
            this.copyType = checkNotNull(builder.copyType);
            this.outputPath = checkNotNull(builder.outputPath);
            this.importPath = checkNotNull(builder.importPath);
            this.useNtpServer = checkNotNull(builder.useNtpServer);
            this.ntpServerHost = checkNotNull(builder.ntpServerHost);
            this.toolStartTime = checkNotNull(builder.toolStartTime);
        } catch(final NullPointerException npe) {
            //fix this.
            throw new MergeConfigurationException("The configuration was missing required field(s)", npe);
        }
    }

    /**
     * @return the hostname of the parent.
     */
    public String getParentHostname() {
        return parentHostname;
    }

    /**
     * @return the username of the parent.
     */
    public String getParentUsername() {
        return parentUsername;
    }

    /**
     * @return the password of the parent.
     */
    public String getParentPassword() {
        return parentPassword;
    }

    /**
     * @return the Rya Instance Name of the parent.
     */
    public String getParentRyaInstanceName() {
        return parentRyaInstanceName;
    }

    /**
     * @return the Rya table prefix of the parent.
     */
    public String getParentTablePrefix() {
        return parentTablePrefix;
    }

    /**
     * @return The URL of the Apache Tomcat server web page running on the parent machine.
     */
    public String getParentTomcatUrl() {
        return parentTomcatUrl;
    }

    /**
     * @return the Database Type of the parent.
     */
    public DBType getParentDBType() {
        return parentDBType;
    }

    /**
     * @return the port of the parent.
     */
    public int getParentPort() {
        return parentPort;
    }

    /**
     * @return the hostname of the child.
     */
    public String getChildHostname() {
        return childHostname;
    }

    /**
     * @return the username of the child.
     */
    public String getChildUsername() {
        return childUsername;
    }

    /**
     * @return the password of the child.
     */
    public String getChildPassword() {
        return childPassword;
    }

    /**
     * @return the Rya Instance Name of the child.
     */
    public String getChildRyaInstanceName() {
        return childRyaInstanceName;
    }

    /**
     * @return the Rya table prefix of the child.
     */
    public String getChildTablePrefix() {
        return childTablePrefix;
    }

    /**
     * @return The URL of the Apache Tomcat server web page running on the child machine.
     */
    public String getChildTomcatUrl() {
        return childTomcatUrl;
    }

    /**
     * @return the Database Type of the child.
     */
    public DBType getChildDBType() {
        return childDBType;
    }

    /**
     * @return the port of the child.
     */
    public int getChildPort() {
        return childPort;
    }

    /**
     * @return the policy to use when merging data.
     */
    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    /**
     * @return the type of copying to perform based on if the datastores are
     * online or offline.
     */
    public CopyType getCopyType() {
        return copyType;
    }

    /**
     * @return the path to output files to when copying to an offline child
     * datastore.
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * @return the path to import files from when copying from an offline parent
     * datastore.
     */
    public String getImportPath() {
        return importPath;
    }

    /**
     * @return {@code true} to use the NTP server to handle time synchronization.
     * {@code false} to not use the NTP server.
     */
    public Boolean getUseNtpServer() {
        return useNtpServer;
    }

    /**
     * @return The host name of the time server to use.
     */
    public String getNtpServerHost() {
        return ntpServerHost;
    }

    /**
     * @return The time of the data to be included in the copy/merge process.
     */
    public String getToolStartTime() {
        return toolStartTime;
    }

    /**
     * Abstract builder to help create {@link MergeConfiguration}s.
     */
    public abstract static class AbstractBuilder<T extends AbstractBuilder<T>> {
        /**
         * @return The {@link MergeConfiguration} based on this builder.
         * @throws MergeConfigurationException
         * @throws NullPointerException if any field as not been provided
         */
        public abstract MergeConfiguration build() throws MergeConfigurationException;
    }

    /**
     * Builder to help create {@link MergeConfiguration}s.
     */
    public static class Builder extends AbstractBuilder<Builder> {
        private String parentHostname;
        private String parentUsername;
        private String parentPassword;
        private String parentRyaInstanceName;
        private String parentTablePrefix;
        private String parentTomcatUrl;
        private DBType parentDBType;
        private Integer parentPort;

        private String childHostname;
        private String childUsername;
        private String childPassword;
        private String childRyaInstanceName;
        private String childTablePrefix;
        private String childTomcatUrl;
        private DBType childDBType;
        private Integer childPort;

        private MergePolicy mergePolicy;
        private CopyType copyType;

        private String outputPath;
        private String importPath;
        private Boolean useNtpServer;
        private String ntpServerHost;
        private String toolStartTime;

        /**
         * @param hostname - the hostname of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentHostname(final String hostname) {
            parentHostname = hostname;
            return this;
        }

        /**
         * @param username - the username of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentUsername(final String username) {
            parentUsername = username;
            return this;
        }

        /**
         * @param password - the password of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentPassword(final String password) {
            parentPassword = password;
            return this;
        }

        /**
         * @param ryaInstanceName - the Rya Instance Name of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentRyaInstanceName(final String ryaInstanceName) {
            parentRyaInstanceName = ryaInstanceName;
            return this;
        }

        /**
         * @param tablePrefix - the Rya table prefix of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentTablePrefix(final String tablePrefix) {
            parentTablePrefix = tablePrefix;
            return this;
        }

        /**
         * @param tomcatUrl - The URL of the Apache Tomcat server web page
         * running on the parent machine.
         * @return the updated {@link Builder}.
         */
        public Builder setParentTomcatUrl(final String tomcatUrl) {
            parentTomcatUrl = tomcatUrl;
            return this;
        }

        /**
         * @param dbType - the Database Type of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentDBType(final DBType dbType) {
            parentDBType = dbType;
            return this;
        }

        /**
         * @param port - the port of the parent.
         * @return the updated {@link Builder}.
         */
        public Builder setParentPort(final Integer port) {
            parentPort = port;
            return this;
        }

        /**
         * @param hostname - the hostname of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildHostname(final String hostname) {
            childHostname = hostname;
            return this;
        }

        /**
         * @param username - the username of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildUsername(final String username) {
            childUsername = username;
            return this;
        }

        /**
         * @param password - the password of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildPassword(final String password) {
            childPassword = password;
            return this;
        }

        /**
         * @param ryaInstanceName - the Rya Instance Name of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildRyaInstanceName(final String ryaInstanceName) {
            childRyaInstanceName = ryaInstanceName;
            return this;
        }

        /**
         * @param tablePrefix - the Rya table prefix of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildTablePrefix(final String tablePrefix) {
            childTablePrefix = tablePrefix;
            return this;
        }

        /**
         * @param tomcatUrl -s The URL of the Apache Tomcat server web page
         * running on the child machine.
         * @return the updated {@link Builder}.
         */
        public Builder setChildTomcatUrl(final String tomcatUrl) {
            childTomcatUrl = tomcatUrl;
            return this;
        }

        /**
         * @param dbType - the Database Type of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildDBType(final DBType dbType) {
            childDBType = dbType;
            return this;
        }

        /**
         * @param port - the port of the child.
         * @return the updated {@link Builder}.
         */
        public Builder setChildPort(final Integer port) {
            childPort = port;
            return this;
        }

        /**
         * @param mergePolicy - the policy to use when merging data.
         * @return the updated {@link Builder}.
         */
        public Builder setMergePolicy(final MergePolicy mergePolicy) {
            this.mergePolicy = mergePolicy;
            return this;
        }

        /**
         * @param copyType - the type of copying to perform based on if the
         * datastores are online or offline.
         * @return the updated {@link Builder}.
         */
        public Builder setCopyType(final CopyType copyType) {
            this.copyType = copyType;
            return this;
        }

        /**
         * @param outpath - the path to output files to when copying to an
         * offline child datastore.
         * @return the updated {@link Builder}.
         */
        public Builder setOutputPath(final String outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        /**
         * @param importPath - the path to import files from when copying from
         * an offline parent datastore.
         * @return the updated {@link Builder}.
         */
        public Builder setImportPath(final String importPath) {
            this.importPath = importPath;
            return this;
        }

        /**
         * @param useNtpServer - {@code true} to use the NTP server to handle
         * time synchronization. {@code false} to not use the NTP server.
         * @return the updated {@link Builder}.
         */
        public Builder setUseNtpServer(final Boolean useNtpServer) {
            this.useNtpServer = useNtpServer;
            return this;
        }

        /**
         * @param ntpServerHost - The host name of the time server to use.
         * @return the updated {@link Builder}.
         */
        public Builder setNtpServerHost(final String ntpServerHost) {
            this.ntpServerHost = ntpServerHost;
            return this;
        }

        /**
         * @param toolStartTime - The time of the data to be included in the
         * copy/merge process.
         * @return the updated {@link Builder}.
         */
        public Builder setToolStartTime(final String toolStartTime) {
            this.toolStartTime = toolStartTime;
            return this;
        }

        @Override
        public MergeConfiguration build() throws MergeConfigurationException {
            return new MergeConfiguration(this);
        }
    }
}
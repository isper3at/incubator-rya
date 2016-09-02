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
package org.apache.rya.export.api.common;

/**
 * Determines the type of merge process that is being run on the statement
 * stores.
 */
public enum MergeProcessType {
    /**
     * The process copies data from the parent to a newly created child
     * datastore.
     */
    COPY,

    /**
     * The process merges data from the child back to the parent comparing
     * differences between the two.
     */
    MERGE;

    /**
     * Finds the merge process type by name.
     * @param name the name to find.
     * @return the {@link MergeProcessType} or {@code null} if none could be
     * found.
     */
    public static MergeProcessType fromName(final String name) {
        for (final MergeProcessType mergeProcessType : MergeProcessType.values()) {
            if (mergeProcessType.toString().equals(name)) {
                return mergeProcessType;
            }
        }
        return null;
    }
}
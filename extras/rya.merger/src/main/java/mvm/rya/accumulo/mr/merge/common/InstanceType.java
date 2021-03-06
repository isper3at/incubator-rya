package mvm.rya.accumulo.mr.merge.common;

/*
 * #%L
 * mvm.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

/**
 * The type of Accumulo instance.
 */
public enum InstanceType {
    /**
     * An Accumulo instance that runs using a regular Accumulo distribution.
     */
    DISTRIBUTION,
    /**
     * An Accumulo instance that runs using a {@link MiniAccumuloCluster}.
     */
    MINI,
    /**
     * An Accumulo instance that runs using a {@link MockInstance}.
     */
    MOCK;

    /**
     * Finds the instance type by name.
     * @param name the name to find.
     * @return the {@link InstanceType} or {@code null} if none could be found.
     */
    public static InstanceType fromName(String name) {
        for (InstanceType instanceType : InstanceType.values()) {
            if (instanceType.toString().equals(name)) {
                return instanceType;
            }
        }
        return null;
    }
}
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
package org.apache.rya.mongodb.document.operators.query;

import java.util.List;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

/**
 * Utility methods for logical operators.
 */
public final class LogicalOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private LogicalOperators() {
    }

    /**
     * Creates an $and MongoDB expression.
     * @param lhs the left-hand side operand.
     * @param rhs the right-hand side operand.
     * @param extras any additional operands.
     * @return the $and expression {@link BasicDBObject}.
     */
    public static BasicDBObject and(final BasicDBObject lhs, final BasicDBObject rhs, final BasicDBObject... extras) {
        final List<BasicDBObject> operands = Lists.newArrayList(lhs, rhs);

        if (extras != null && extras.length > 0) {
            operands.addAll(Lists.newArrayList(extras));
        }

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$and", operands);
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates an $or MongoDB expression.
     * @param lhs the left-hand side operand.
     * @param rhs the right-hand side operand.
     * @param extras any additional operands.
     * @return the $or expression {@link BasicDBObject}.
     */
    public static BasicDBObject or(final BasicDBObject lhs, final BasicDBObject rhs, final BasicDBObject... extras) {
        final List<BasicDBObject> operands = Lists.newArrayList(lhs, rhs);

        if (extras != null && extras.length > 0) {
            operands.addAll(Lists.newArrayList(extras));
        }

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$or", operands);
        return (BasicDBObject) builder.get();
    }
}
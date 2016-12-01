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

import java.util.Arrays;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

/**
 * Utility methods for conditional operators.
 */
public final class ConditionalOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private ConditionalOperators() {
    }

    /**
     * Creates an "if-then-else" MongoDB expression.
     * @param ifStatement the "if" statement {@link BasicDBObject}.
     * @param thenResult the {@link Object} to return when the
     * {@code ifStatement} is {@code true}.
     * @param elseResult the {@link Object} to return when the
     * {@code ifStatement} is {@code false}.
     * @return the "if" expression {@link BasicDBObject}.
     */
    public static BasicDBObject ifThenElse(final BasicDBObject ifStatement, final Object thenResult, final Object elseResult) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("if", ifStatement);
        builder.append("then", thenResult);
        builder.append("else", elseResult);
        return (BasicDBObject) builder.get();
    }

    /**
     * Checks if the expression is {@code null} and replaces it if it is.
     * @param expression the expression to {@code null} check.
     * @param replacementExpression the expression to replace it with if it's
     * {@code null}.
     * @return the $ifNull expression {@link BasicDBObject}.
     */
    public static BasicDBObject ifNull(final Object expression, final Object replacementExpression) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$ifNull", Arrays.asList(expression, replacementExpression));
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates an "$cond" MongoDB expression.
     * @param expression the expression {@link BasicDBObject}.
     * @param thenResult the {@link Object} to return when the
     * {@code expression} is {@code true}.
     * @param elseResult the {@link Object} to return when the
     * {@code expression} is {@code false}.
     * @return the $cond expression {@link BasicDBObject}.
     */
    public static BasicDBObject cond(final BasicDBObject expression, final Object thenResult, final Object elseResult) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$cond", ifThenElse(expression, thenResult, elseResult));
        return (BasicDBObject) builder.get();
    }
}
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
package org.apache.rya.mongodb.iter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.util.AuthorizationsUtil;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Utility methods for MongoDB aggregation.
 */
public final class AggregationUtil {
    /**
     * Private constructor to prevent instantiation.
     */
    private AggregationUtil() {
    }

    /**
     * Creates a MongoDB $redact aggregation pipeline that only include
     * documents whose document visibility match the provided authorizations.
     * All other documents are excluded.
     * @param authorizations the {@link Authorization}s to include in the
     * $redact. Only documents that match the authorizations will be returned.
     * @return the {@link List} of {@link DBObject}s that represents the $redact
     * aggregation pipeline.
     */
    public static List<DBObject> createRedactPipeline(final Authorizations authorizations) {
        if (MongoDbRdfConstants.ALL_AUTHORIZATIONS.equals(authorizations)) {
            return Lists.newArrayList();
        }

        final List<String> authAndList = AuthorizationsUtil.getAuthorizationsStrings(authorizations);

        // Generate all combinations of the authorization strings without repetition.
        final List<List<String>> authOrList = createCombinations(AuthorizationsUtil.getAuthorizationsStrings(authorizations));

        final String documentVisibilityField = "$" + SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;

        final BasicDBObject setIsSubset =
            setIsSubsetNullSafe(
                documentVisibilityField,
                authAndList.toArray()
            );

        final BasicDBObject setIntersectionExists =
            gt(
                size(
                    setIntersection(
                        documentVisibilityField,
                        authOrList.toArray()
                    )
                ),
                0
            );

        final BasicDBObject orExpression = or(setIsSubset, setIntersectionExists);

        final List<DBObject> pipeline = new ArrayList<>();
        pipeline.add(
            redact(
               orExpression,
               "$$DESCEND",
               "$$PRUNE"
            )
        );

        return pipeline;
    }

    /**
     * Creates all combinations of the values that are of the size of value
     * array or smaller without repetition.
     * @param values the {@link List} of values to create combinations from.
     * @return the {@link List} of combinations.
     */
    private static <T> List<List<T>> createCombinations(final List<T> values) {
        final List<List<T>> allCombinations = new ArrayList<>();
        for (int i = 1; i <= values.size(); i++) {
            allCombinations.addAll(createCombinations(values, i));
        }
        return allCombinations;
    }

    /**
     * Creates all combinations of the values that are of the specified size
     * without repetition.
     * @param values the {@link List} of values to create combinations from.
     * @param size the size of the combinations.
     * @return the {@link List} of combinations.
     */
    private static <T> List<List<T>> createCombinations(final List<T> values, final int size) {
        if (0 == size) {
            return Collections.singletonList(Collections.<T> emptyList());
        }

        if (values.isEmpty()) {
            return Collections.emptyList();
        }

        final List<List<T>> combination = new LinkedList<List<T>>();

        final T actual = values.iterator().next();

        final List<T> subSet = new LinkedList<T>(values);
        subSet.remove(actual);

        final List<List<T>> subSetCombination = createCombinations(subSet, size - 1);

        for (final List<T> set : subSetCombination) {
            final List<T> newSet = new LinkedList<T>(set);
            newSet.add(0, actual);
            combination.add(newSet);
        }

        combination.addAll(createCombinations(subSet, size));

        return combination;
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
    private static BasicDBObject ifThenElse(final BasicDBObject ifStatement, final Object thenResult, final Object elseResult) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("if", ifStatement);
        builder.append("then", thenResult);
        builder.append("else", elseResult);
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
    private static BasicDBObject cond(final BasicDBObject expression, final Object thenResult, final Object elseResult) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$cond", ifThenElse(expression, thenResult, elseResult));
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates an $and MongoDB expression.
     * @param lhs the left-hand side operand.
     * @param rhs the right-hand side operand.
     * @param extras any additional operands.
     * @return the $and expression {@link BasicDBObject}.
     */
    private static BasicDBObject and(final BasicDBObject lhs, final BasicDBObject rhs, final BasicDBObject... extras) {
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
    private static BasicDBObject or(final BasicDBObject lhs, final BasicDBObject rhs, final BasicDBObject... extras) {
        final List<BasicDBObject> operands = Lists.newArrayList(lhs, rhs);

        if (extras != null && extras.length > 0) {
            operands.addAll(Lists.newArrayList(extras));
        }

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$or", operands);
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates an $size MongoDB expression.
     * @param expression the expression to get the size of.
     * @return the $size expression {@link BasicDBObject}.
     */
    private static BasicDBObject size(final BasicDBObject expression) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$size", expression);
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates an $gt MongoDB expression.
     * @param expression the expression.
     * @param the value to test if the expression is greater than
     * @return the $gt expression {@link BasicDBObject}.
     */
    private static BasicDBObject gt(final BasicDBObject expression, final Number value) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$gt", Arrays.asList(expression, value));
        return (BasicDBObject) builder.get();
    }

    /**
     * Checks if the field intersects the set.
     * @param field the field to check.
     * @param set the set to check.
     * @return the $setIntersection expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIntersection(final String field, final Object[] set) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$setIntersection", Arrays.asList(field, set));
        return (BasicDBObject) builder.get();
    }

    /**
     * Checks if the expression is a subset of the set.
     * @param expression the expression to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIsSubset(final DBObject expression, final Object[] set) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$setIsSubset", Arrays.asList(expression, set).toArray(new Object[0]));
        return (BasicDBObject) builder.get();
    }

    /**
     * Checks if the field is a subset of the set and is safe if the field is
     * null.
     * @param field the field to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIsSubsetNullSafe(final String field, final Object[] set) {
        final Object emptyAccess = Collections.emptyList().toArray();
        return setIsSubset(
            ifNull(
                field,
                emptyAccess
            ),
            set
        );
    }

    /**
     * Checks if the expression is {@code null} and replaces it if it is.
     * @param expression the expression to {@code null} check.
     * @param replacementExpression the expression to replace it with if it's
     * {@code null}.
     * @return the $ifNull expression {@link BasicDBObject}.
     */
    private static BasicDBObject ifNull(final Object expression, final Object replacementExpression) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$ifNull", Arrays.asList(expression, replacementExpression));
        return (BasicDBObject) builder.get();
    }

    /**
     * Creates a $redact expression.
     * @param expression the expression to run redact on.
     * @param acceptResult the result to return when the expression
     * passes.
     * @param rejectResult the result to return when the expression
     * fails.
     * @return the $redact expression {@link BasicDBObject}.
     */
    public static BasicDBObject redact(final BasicDBObject expression, final String acceptResult, final String rejectResult) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("$redact", cond(expression, acceptResult, rejectResult));
        return (BasicDBObject) builder.get();
    }
}
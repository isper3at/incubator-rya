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
package org.apache.rya.export.accumulo.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.ListMemberOperator;
import org.openrdf.query.algebra.Or;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.sail.SailException;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.inference.InferJoin;
import mvm.rya.rdftriplestore.inference.InferUnion;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.inference.InverseOfVisitor;
import mvm.rya.rdftriplestore.inference.SameAsVisitor;
import mvm.rya.rdftriplestore.inference.SubClassOfVisitor;
import mvm.rya.rdftriplestore.inference.SubPropertyOfVisitor;
import mvm.rya.rdftriplestore.inference.SymmetricPropertyVisitor;
import mvm.rya.rdftriplestore.inference.TransitivePropertyVisitor;
import mvm.rya.rdftriplestore.utils.FixedStatementPattern;
import mvm.rya.rdftriplestore.utils.TransitivePropertySP;
import mvm.rya.sail.config.RyaSailFactory;

/**
 * Represents a set of {@link CopyRule} instances derived from a query. The ruleset determines a logical
 * subset of statements in Rya, such that statements selected by the ruleset are at least enough to answer
 * the query.
 */
public class QueryRuleset {
    private static final Logger log = Logger.getLogger(QueryRuleset.class);

    /**
     * Represents an error attempting to convert a query to a set of rules.
     */
    public static class QueryRulesetException extends Exception {
        private static final long serialVersionUID = 1L;
        public QueryRulesetException(String s) {
            super(s);
        }
        public QueryRulesetException(String s, Throwable throwable) {
            super(s, throwable);
        }
    }

    /**
     * Takes in a parsed query tree and extracts the rules defining relevant statements.
     */
    private static class RulesetVisitor extends QueryModelVisitorBase<QueryRulesetException> {
        List<CopyRule> rules = new LinkedList<>();
        private Set<Value> superclasses = new HashSet<>();
        private Set<Value> superproperties = new HashSet<>();
        private Set<Value> sameAs = new HashSet<>();
        private Set<Value> transitive = new HashSet<>();
        private Set<Value> schemaProperties = new HashSet<>();

        @Override
        public void meet(StatementPattern node) throws QueryRulesetException {
            Var predVar = node.getPredicateVar();
            // If this is a transitive property node, just match all statements with that property
            if (node instanceof TransitivePropertySP && predVar.hasValue()) {
                node = new StatementPattern(new Var("transitiveSubject"), predVar,
                        new Var("transitiveObject"), node.getContextVar());
                // And make sure to grab the transitivity statement itself
                transitive.add(predVar.getValue());
            }
            rules.add(new CopyRule(node));
        }

        @Override
        public void meet(Filter node) throws QueryRulesetException {
            ValueExpr condition = node.getCondition();
            // If the condition is a function call, and we don't know about the function, don't try to test for it.
            if (condition instanceof FunctionCall) {
                String uri = ((FunctionCall) condition).getURI();
                if (FunctionRegistry.getInstance().get(uri) == null) {
                    // Just extract statement patterns from the child as if there were no filter.
                    node.getArg().visit(this);
                }
            }
            // Otherwise, assume we can test for it: extract rules from below this node, and add the condition to each one.
            else {
                RulesetVisitor childVisitor = new RulesetVisitor();
                node.getArg().visit(childVisitor);
                for (CopyRule rule : childVisitor.rules) {
                    rule.addCondition(condition);
                    this.rules.add(rule);
                }
                this.superclasses.addAll(childVisitor.superclasses);
                this.superproperties.addAll(childVisitor.superproperties);
            }
        }

        @Override
        public void meet(Join node) throws QueryRulesetException {
            TupleExpr left = node.getLeftArg();
            TupleExpr right = node.getRightArg();
            // If this join represents the application of inference logic, use its children to add the
            // appropriate rules.
            if (node instanceof InferJoin && left instanceof FixedStatementPattern) {
                FixedStatementPattern fsp = (FixedStatementPattern) left;
                Value predValue = fsp.getPredicateVar().getValue();
                // If this is a subClassOf relation, fetch all subClassOf and equivalentClass
                // relations involving the relevant classes.
                if (RDFS.SUBCLASSOF.equals(predValue) && right instanceof StatementPattern) {
                    StatementPattern dne = (StatementPattern) right;
                    // If a subClassOf b equivalentClass c subClassOf d, then fsp will contain a statement
                    // for each class in the hierarchy. If we match every subClassOf and equivalentClass
                    // relation to any of {a,b,c,d}, then the hierarchy can be reconstructed.
                    for (Statement st : fsp.statements) {
                        Value superclassVal = st.getSubject();
                        // Rule to match the type assignment:
                        rules.add(new CopyRule(new StatementPattern(dne.getSubjectVar(),
                                dne.getPredicateVar(),
                                new Var(superclassVal.toString(), superclassVal),
                                dne.getContextVar())));
                        // Add to the set of classes for which we need the hierarchy:
                        superclasses.add(superclassVal);
                    }
                }
                // If this is a subPropertyOf relation, fetch all subPropertyOf and equivalentProperty
                // relations involving the relevant properties.
                else if (RDFS.SUBPROPERTYOF.equals(predValue) && right instanceof StatementPattern) {
                    StatementPattern dne = (StatementPattern) right;
                    // If p subPropertyOf q subPropertyOf r subPropertyOf s, then fsp will contain a statement
                    // for each property in the hierarchy. If we match every subPropertyOf and equivalentProperty
                    // relation to any of {p,q,r,s}, then the hierarchy can be reconstructed.
                    for (Statement st : fsp.statements) {
                        Value superpropVal = st.getSubject();
                        // Rule to add the property:
                        rules.add(new CopyRule(new StatementPattern(dne.getSubjectVar(),
                                new Var(superpropVal.toString(), superpropVal),
                                dne.getObjectVar(),
                                dne.getContextVar())));
                        // Add to the set of properties for which we need the hierarchy:
                        superproperties.add(superpropVal);
                    }
                }
                // If this is a sameAs expansion, it may have one or two levels
                if (OWL.SAMEAS.equals(predValue)) {
                    StatementPattern stmt = null;
                    String replaceVar = fsp.getSubjectVar().getName();
                    String replaceVarInner = null;
                    List<Value> replacements = new LinkedList<>();
                    List<Value> replacementsInner = new LinkedList<>();
                    for (Statement st : fsp.statements) {
                        replacements.add(st.getSubject());
                    }
                    if (right instanceof StatementPattern) {
                        stmt = (StatementPattern) right;
                    }
                    else if (right instanceof InferJoin) {
                        // Add the second set of replacements if given
                        InferJoin inner = (InferJoin) right;
                        if (inner.getLeftArg() instanceof FixedStatementPattern
                                && inner.getRightArg() instanceof StatementPattern) {
                            stmt = (StatementPattern) inner.getRightArg();
                            fsp = (FixedStatementPattern) inner.getLeftArg();
                            replaceVarInner = fsp.getSubjectVar().getName();
                            for (Statement st : fsp.statements) {
                                replacementsInner.add(st.getSubject());
                            }
                        }
                    }
                    // Add different versions of the original statement:
                    if (stmt != null) {
                        for (Value replacementVal : replacements) {
                            if (replacementsInner.isEmpty()) {
                                StatementPattern transformed = stmt.clone();
                                if (transformed.getSubjectVar().equals(replaceVar)) {
                                    transformed.setSubjectVar(new Var(replaceVar, replacementVal));
                                }
                                if (transformed.getObjectVar().equals(replaceVar)) {
                                    transformed.setObjectVar(new Var(replaceVar, replacementVal));
                                }
                                rules.add(new CopyRule(transformed));
                            }
                            for (Value replacementValInner : replacementsInner) {
                                StatementPattern transformed = stmt.clone();
                                if (transformed.getSubjectVar().equals(replaceVar)) {
                                    transformed.setSubjectVar(new Var(replaceVar, replacementVal));
                                }
                                else if (transformed.getSubjectVar().equals(replaceVarInner)) {
                                    transformed.setSubjectVar(new Var(replaceVarInner, replacementValInner));
                                }
                                if (transformed.getObjectVar().equals(replaceVar)) {
                                    transformed.setObjectVar(new Var(replaceVar, replacementVal));
                                }
                                else if (transformed.getObjectVar().equals(replaceVarInner)) {
                                    transformed.setObjectVar(new Var(replaceVar, replacementValInner));
                                }
                                rules.add(new CopyRule(transformed));
                            }
                        }
                    }
                    // Add to the set of resources for which we need sameAs relations:
                    sameAs.addAll(replacements);
                    sameAs.addAll(replacementsInner);
                }
            }
            // If it's a normal join, visit the children.
            else {
                super.meet(node);
            }
        }

        @Override
        public void meet(Union node) throws QueryRulesetException {
            node.visitChildren(this);
            if (node instanceof InferUnion) {
                // If this is the result of inference, search each tree for (non-standard) properties and add them
                // to the set of properties for which to include schema information.
                QueryModelVisitorBase<QueryRulesetException> propertyVisitor = new QueryModelVisitorBase<QueryRulesetException>() {
                    @Override
                    public void meet(StatementPattern node) {
                        if (node.getPredicateVar().hasValue()) {
                            URI predValue = (URI) node.getPredicateVar().getValue();
                            String ns = predValue.getNamespace();
                            if (node instanceof FixedStatementPattern
                                    && (RDFS.SUBPROPERTYOF.equals(predValue) || OWL.EQUIVALENTPROPERTY.equals(predValue))) {
                                // This FSP replaced a property, so find all the properties it entails
                                FixedStatementPattern fsp = (FixedStatementPattern) node;
                                for (Statement stmt : fsp.statements) {
                                    schemaProperties.add(stmt.getSubject());
                                }
                            }
                            else if (!(OWL.NAMESPACE.equals(ns) || RDFS.NAMESPACE.equals(ns) || RDF.NAMESPACE.equals(ns))) {
                                // This is a regular triple pattern; grab its predicate
                                schemaProperties.add(predValue);
                            }
                        }
                    }
                };
                node.getLeftArg().visit(propertyVisitor);
                node.getRightArg().visit(propertyVisitor);
            }
        }

        /**
         * Add rules covering the portions of the schema that may be necessary to use inference
         * with this query.
         */
        public void addSchema() throws QueryRulesetException {
            // Combine the relevant portions of the class hierarchy into one subclass rule and one equivalent class rule:
            if (!superclasses.isEmpty()) {
                Var superClassVar = new Var("superClassVar");
                // Subclasses of the given classes:
                addListRule(new Var("subClassVar"), null, RDFS.SUBCLASSOF, superClassVar, superclasses);
                // Equivalent classes to the given classes (this might be stated in either direction):
                addListRule(new Var("eqClassSubject"), superclasses, OWL.EQUIVALENTCLASS, new Var("eqClassObject"), superclasses);
            }

            // Combine the relevant portions of the property hierarchy into one subproperty rule and one equivalent property rule:
            if (!superproperties.isEmpty()) {
                Var superPropertyVar = new Var("superPropertyVar");
                // Subproperties of the given properties:
                addListRule(new Var("subPropertyVar"), null, RDFS.SUBPROPERTYOF, superPropertyVar, superproperties);
                // Equivalent properties to the given properties (this might be stated in either direction):
                addListRule(new Var("eqPropSubject"), superproperties, OWL.EQUIVALENTPROPERTY, new Var("eqPropObject"), superproperties);
            }

            // Get the relevant portions of the owl:sameAs graph
            if (!sameAs.isEmpty()) {
                Var sameAsSubj = new Var("sameAsSubject");
                Var sameAsObj = new Var("sameAsObject");
                addListRule(sameAsSubj, sameAs, OWL.SAMEAS, sameAsObj, sameAs);
            }

            // Get the potentially relevant owl:TransitiveProperty statements
            if (!transitive.isEmpty()) {
                Var transitiveVar = new Var(OWL.TRANSITIVEPROPERTY.toString(), OWL.TRANSITIVEPROPERTY);
                addListRule(new Var("transitiveProp"), transitive, RDF.TYPE, transitiveVar, null);
            }

            // Get any owl:SymmetricProperty and owl:inverseOf statements for relevant properties
            if (!schemaProperties.isEmpty()) {
                Var symmetricVar = new Var(OWL.SYMMETRICPROPERTY.toString(), OWL.SYMMETRICPROPERTY);
                addListRule(new Var("symmetricProp"), schemaProperties, RDF.TYPE, symmetricVar, null);
                addListRule(new Var("inverseSubject"), schemaProperties, OWL.INVERSEOF, new Var("inverseObject"), schemaProperties);
            }
        }

        /**
         * Build and add a rule that matches triples having a specific predicate, where subject and object constraints
         * are each defined using a Var and a set of Values, and each can represent one of: a constant value
         * (Var has a value), an enumerated set of possible values, to be turned into a filter (Var has no
         * Value and set of Values is non-null), or an unconstrained variable (Var has no value and set of
         * Values is null). If both subject and object are variables with enumerated sets, only one part needs to
         * match in order to accept the triple.
         * @param subjVar Var corresponding to the subject. May have a specific value or represent a variable.
         * @param subjValues Either null or a Set of Values that the subject variable can have, tested using a filter.
         * @param predicate The URI for the predicate to match
         * @param objVar Var corresponding to the object. May have a specific value or represent a variable.
         * @param objValues Either null or a Set of Values that the object variable can have, tested using a filter
         * @throws QueryRulesetException if the rule can't be created
         */
        private void addListRule(Var subjVar, Set<Value> subjValues, URI predicate,
                Var objVar, Set<Value> objValues) throws QueryRulesetException {
            ListMemberOperator subjCondition = null;
            ListMemberOperator objCondition = null;
            if (subjValues != null) {
                subjCondition = new ListMemberOperator();
                subjCondition.addArgument(subjVar);
                for (Value constant : subjValues) {
                    subjCondition.addArgument(new Var(constant.toString(), constant));
                }
            }
            if (objValues != null) {
                objCondition = new ListMemberOperator();
                objCondition.addArgument(objVar);
                for (Value constant : objValues) {
                    objCondition.addArgument(new Var(constant.toString(), constant));
                }
            }
            Var predVar = new Var(predicate.toString(), predicate);
            CopyRule listRule = new CopyRule(new StatementPattern(subjVar, predVar, objVar));
            if (subjCondition != null && objCondition != null) {
                listRule.addCondition(new Or(subjCondition, objCondition));
            }
            else if (subjCondition != null) {
                listRule.addCondition(subjCondition);
            }
            else if (objCondition != null) {
                listRule.addCondition(objCondition);
            }
            rules.add(listRule);
        }
    }

    /**
     * The rules themselves -- any statement satisfying any of these rules will be copied.
     */
    protected Set<CopyRule> rules = new HashSet<>();

    /**
     * The SPARQL query that defines the ruleset.
     */
    protected String query;

    /**
     * A Rya configuration.
     */
    protected RdfCloudTripleStoreConfiguration conf;

    /**
     * Extract a set of rules from a query found in a Configuration.
     * @param conf Configuration containing either the query string, or name of a file containing the query, plus inference parameters.
     * @throws QueryRulesetException if the query can't be read, parsed, and resolved to valid rules
     */
    public QueryRuleset(RdfCloudTripleStoreConfiguration conf) throws QueryRulesetException {
        this.conf = conf;
        setQuery();
        setRules();
    }

    /**
     * Extract a set of rules from a query.
     * @param query A SPARQL query string
     * @throws QueryRulesetException if the query can't be parsed and resolved to valid rules
     */
    public QueryRuleset(String query) throws QueryRulesetException {
        this.query = query;
        setRules();
    }

    /**
     * Get the query that was used to construct this ruleset.
     * @return A SPARQL query
     */
    public String getQuery() {
        return query;
    }

    /**
     * Set this ruleset's defining query based on the configuration. Query can be
     * specified directly or using a file; if it's read from a file, the query
     * text will also be added to the configuration.
     * @return SPARQL query
     * @throws QueryRulesetException if there is no configuration, or if the query can't be found or read
     */
    private void setQuery() throws QueryRulesetException {
        if (conf == null) {
            throw new QueryRulesetException("No Configuration given");
        }
        query = conf.get(AccumuloExportConstants.QUERY_STRING_PROP);
        String queryFile = conf.get(AccumuloExportConstants.QUERY_FILE_PROP);
        if (query == null && queryFile != null) {
            try {
                FileReader fileReader = new FileReader(queryFile);
                BufferedReader reader = new BufferedReader(fileReader);
                StringBuilder builder = new StringBuilder();
                String line = reader.readLine();
                while (line != null) {
                    builder.append(line).append("\n");
                    line = reader.readLine();
                }
                query = builder.toString();
                reader.close();
                conf.set(AccumuloExportConstants.QUERY_STRING_PROP, query);
            }
            catch (IOException e) {
                throw new QueryRulesetException("Error loading query from file: " + queryFile, e);
            }
        }
        else if (query == null) {
            throw new QueryRulesetException("No query string or query file provided");
        }
    }

    /**
     * Extract the rules from the query string, applying inference rules if configured to.
     * @throws QueryRulesetException if the parsed query can't be parsed and translated into valid rules.
     */
    private void setRules() throws QueryRulesetException {
        final ParsedTupleQuery ptq;
        final TupleExpr te;
        try {
            ptq = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null);
        }
        catch (UnsupportedQueryLanguageException | MalformedQueryException e) {
            throw new QueryRulesetException("Error parsing query:\n" + query, e);
        }
        te = ptq.getTupleExpr();
        // Before converting to rules (and renaming variables), validate that no statement patterns
        // consist of only variables (this would result in a rule  that matches every triple).
        // Needs to be done before inference, since inference rules may create such statement patterns
        // that are OK because they won'd be converted to rules directly.
        te.visit(new QueryModelVisitorBase<QueryRulesetException>() {
            @Override
            public void meet(StatementPattern node) throws QueryRulesetException {
                if (!(node.getSubjectVar().hasValue() || node.getPredicateVar().hasValue() || node.getObjectVar().hasValue())) {
                    throw new QueryRulesetException("Statement pattern with no constants would match every statement:\n"
                            + node + "\nFrom parsed query:\n" + te);
                }
            }
        });
        // Apply inference, if applicable
        if (conf != null && conf.isInfer()) {
            RdfCloudTripleStore store = null;
            try {
                log.info("Applying inference rules");
                store = (RdfCloudTripleStore) RyaSailFactory.getInstance(conf);
                InferenceEngine inferenceEngine = store.getInferenceEngine();
                // Apply in same order as query evaluation:
                te.visit(new TransitivePropertyVisitor(conf, inferenceEngine));
                te.visit(new SymmetricPropertyVisitor(conf, inferenceEngine));
                te.visit(new InverseOfVisitor(conf, inferenceEngine));
                te.visit(new SubPropertyOfVisitor(conf, inferenceEngine));
                te.visit(new SubClassOfVisitor(conf, inferenceEngine));
                te.visit(new SameAsVisitor(conf, inferenceEngine));
                log.info("Query after inference:\n");
                for (String line : te.toString().split("\n")) {
                    log.info("\t" + line);
                }
            }
            catch (Exception e) {
                throw new QueryRulesetException("Error applying inference to parsed query:\n" + te, e);
            }
            finally {
                if (store != null) {
                    try {
                        store.shutDown();
                    } catch (SailException e) {
                        log.error("Error shutting down Sail after applying inference", e);
                    }
                }
            }
        }
        // Extract the StatementPatterns and Filters and turn them into rules:
        RulesetVisitor rv = new RulesetVisitor();
        try {
            te.visit(rv);
            rv.addSchema();
        }
        catch (QueryRulesetException e) {
            throw new QueryRulesetException("Error extracting rules from parsed query:\n" + te, e);
        }
        for (CopyRule candidateRule : rv.rules) {
            boolean unique = true;
            for (CopyRule otherRule : rv.rules) {
                if (!candidateRule.equals(otherRule) && otherRule.isGeneralizationOf(candidateRule)) {
                    unique = false;
                    break;
                }
            }
            if (unique) {
                this.rules.add(candidateRule);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Original Query:\n\n\t");
        sb.append(query.replace("\n", "\n\t")).append("\n\nRuleset:\n");
        for (CopyRule rule : rules) {
            sb.append("\n\t").append(rule.toString().replace("\n", "\n\t")).append("\n");
        }
        return sb.toString();
    }
}

/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.filter;

import com.espertech.esper.client.ConfigurationInformation;
import com.espertech.esper.client.EventType;
import com.espertech.esper.collection.Pair;
import com.espertech.esper.core.context.util.ContextDescriptor;
import com.espertech.esper.core.service.ExprEvaluatorContextStatement;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.core.start.EPStatementStartMethodHelperSubselect;
import com.espertech.esper.core.start.EPStatementStartMethodHelperValidate;
import com.espertech.esper.epl.core.MethodResolutionService;
import com.espertech.esper.epl.core.StreamTypeService;
import com.espertech.esper.epl.core.StreamTypeServiceImpl;
import com.espertech.esper.epl.core.ViewResourceDelegateUnverified;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeUtil;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.expression.subquery.ExprSubselectNode;
import com.espertech.esper.epl.expression.visitor.ExprNodeSubselectDeclaredDotVisitor;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.named.NamedWindowService;
import com.espertech.esper.epl.property.PropertyEvaluator;
import com.espertech.esper.epl.property.PropertyEvaluatorFactory;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.epl.table.mgmt.TableService;
import com.espertech.esper.epl.util.EPLValidationUtil;
import com.espertech.esper.epl.variable.VariableService;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.schedule.TimeProvider;
import com.espertech.esper.view.ViewFactoryChain;
import com.espertech.esper.view.ViewProcessingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * Helper to compile (validate and optimize) filter expressions as used in pattern and filter-based streams.
 */
public final class FilterSpecCompiler
{
    private static final Log log = LogFactory.getLog(FilterSpecCompiler.class);

    /**
     * Assigned for filter parameters that are based on boolean expression and not on
     * any particular property name.
     * <p>
     * Keeping this artificial property name is a simplification as optimized filter parameters
     * generally keep a property name.
     */
    public final static String PROPERTY_NAME_BOOLEAN_EXPRESSION = ".boolean_expression";

    /**
     * Factory method for compiling filter expressions into a filter specification
     * for use with filter service.
     * @param eventType is the filtered-out event type
     * @param eventTypeName is the name of the event type
     * @param filterExpessions is a list of filter expressions
     * @param taggedEventTypes is a map of stream names (tags) and event types available
     * @param arrayEventTypes is a map of name tags and event type per tag for repeat-expressions that generate an array of events
     * @param streamTypeService is used to set rules for resolving properties
     * @param optionalStreamName - the stream name, if provided
     * @param optionalPropertyEvalSpec - specification for evaluating properties
     * @param statementContext context for statement
     * @return compiled filter specification
     * @throws ExprValidationException if the expression or type validations failed
     */
    public static FilterSpecCompiled makeFilterSpec(EventType eventType,
                                                    String eventTypeName,
                                                    List<ExprNode> filterExpessions,
                                                    PropertyEvalSpec optionalPropertyEvalSpec,
                                                    LinkedHashMap<String, Pair<EventType, String>> taggedEventTypes,
                                                    LinkedHashMap<String, Pair<EventType, String>> arrayEventTypes,
                                                    StreamTypeService streamTypeService,
                                                    String optionalStreamName,
                                                    StatementContext statementContext,
                                                    Collection<Integer> assignedTypeNumberStack)
            throws ExprValidationException
    {
        // Validate all nodes, make sure each returns a boolean and types are good;
        // Also decompose all AND super nodes into individual expressions
        List<ExprNode> validatedNodes = validateAllowSubquery(ExprNodeOrigin.FILTER, filterExpessions, streamTypeService, statementContext, taggedEventTypes, arrayEventTypes);
        return build(validatedNodes, eventType, eventTypeName, optionalPropertyEvalSpec, taggedEventTypes, arrayEventTypes, streamTypeService, optionalStreamName, statementContext, assignedTypeNumberStack);
    }

    public static FilterSpecCompiled build(List<ExprNode> validatedNodes,
                                            EventType eventType,
                                            String eventTypeName,
                                            PropertyEvalSpec optionalPropertyEvalSpec,
                                            LinkedHashMap<String, Pair<EventType, String>> taggedEventTypes,
                                            LinkedHashMap<String, Pair<EventType, String>> arrayEventTypes,
                                            StreamTypeService streamTypeService,
                                            String optionalStreamName,
                                            StatementContext stmtContext,
                                            Collection<Integer> assignedTypeNumberStack) throws ExprValidationException {

        ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(stmtContext, false);

        return buildNoStmtCtx(validatedNodes, eventType, eventTypeName, optionalPropertyEvalSpec, taggedEventTypes, arrayEventTypes, streamTypeService,
                optionalStreamName, assignedTypeNumberStack,
                evaluatorContextStmt, stmtContext.getStatementId(), stmtContext.getStatementName(), stmtContext.getAnnotations(), stmtContext.getContextDescriptor(),
                stmtContext.getMethodResolutionService(), stmtContext.getEventAdapterService(), stmtContext.getTimeProvider(), stmtContext.getVariableService(), stmtContext.getTableService(), stmtContext.getConfigSnapshot(), stmtContext.getNamedWindowService());
    }

    public static FilterSpecCompiled buildNoStmtCtx(List<ExprNode> validatedFilterNodes,
                                            EventType eventType,
                                            String eventTypeName,
                                            PropertyEvalSpec optionalPropertyEvalSpec,
                                            LinkedHashMap<String, Pair<EventType, String>> taggedEventTypes,
                                            LinkedHashMap<String, Pair<EventType, String>> arrayEventTypes,
                                            StreamTypeService streamTypeService,
                                            String optionalStreamName,
                                            Collection<Integer> assignedTypeNumberStack,
                                            ExprEvaluatorContext exprEvaluatorContext,
                                            String statementId,
                                            String statementName,
                                            Annotation[] annotations,
                                            ContextDescriptor contextDescriptor,
                                            MethodResolutionService methodResolutionService,
                                            EventAdapterService eventAdapterService,
                                            TimeProvider timeProvider,
                                            VariableService variableService,
                                            TableService tableService,
                                            ConfigurationInformation configurationInformation,
                                            NamedWindowService namedWindowService) throws ExprValidationException {

        FilterSpecCompilerArgs args = new FilterSpecCompilerArgs(taggedEventTypes, arrayEventTypes, exprEvaluatorContext, statementName, statementId, streamTypeService, methodResolutionService, timeProvider, variableService, tableService, eventAdapterService, annotations, contextDescriptor, configurationInformation);
        List<FilterSpecParam>[] parameters = FilterSpecCompilerPlanner.planFilterParameters(validatedFilterNodes, args);

        PropertyEvaluator optionalPropertyEvaluator = null;
        if (optionalPropertyEvalSpec != null) {
            optionalPropertyEvaluator = PropertyEvaluatorFactory.makeEvaluator(optionalPropertyEvalSpec, eventType, optionalStreamName, eventAdapterService, methodResolutionService, timeProvider, variableService, tableService, streamTypeService.getEngineURIQualifier(), statementId, statementName, annotations, assignedTypeNumberStack, configurationInformation, namedWindowService);
        }

        FilterSpecCompiled spec = new FilterSpecCompiled(eventType, eventTypeName, parameters, optionalPropertyEvaluator);

        if (log.isDebugEnabled()) {
            log.debug(".makeFilterSpec spec=" + spec);
        }
        return spec;
    }

    /**
     * Validates expression nodes and returns a list of validated nodes.
     * @param exprNodes is the nodes to validate
     * @param streamTypeService is provding type information for each stream
     * @param taggedEventTypes pattern tagged types
     * @param arrayEventTypes @return list of validated expression nodes
     * @return expr nodes
     * @param statementContext context
     * @throws ExprValidationException for validation errors
     */
    public static List<ExprNode> validateAllowSubquery(ExprNodeOrigin exprNodeOrigin,
                                                       List<ExprNode> exprNodes, StreamTypeService streamTypeService,
                                                       StatementContext statementContext,
                                                       LinkedHashMap<String, Pair<EventType, String>> taggedEventTypes,
                                                       LinkedHashMap<String, Pair<EventType, String>> arrayEventTypes)
            throws ExprValidationException
    {
        List<ExprNode> validatedNodes = new ArrayList<ExprNode>();

        ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(statementContext, false);
        ExprValidationContext validationContext = new ExprValidationContext(streamTypeService, statementContext.getMethodResolutionService(), null, statementContext.getTimeProvider(), statementContext.getVariableService(), statementContext.getTableService(), evaluatorContextStmt, statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, true);
        for (ExprNode node : exprNodes)
        {
            // Determine subselects
            ExprNodeSubselectDeclaredDotVisitor visitor = new ExprNodeSubselectDeclaredDotVisitor();
            node.accept(visitor);

            // Compile subselects
            if (!visitor.getSubselects().isEmpty()) {

                // The outer event type is the filtered-type itself
                int subselectStreamNumber = 2048;
                int count = -1;
                for (ExprSubselectNode subselect : visitor.getSubselects()) {
                    count++;
                    subselectStreamNumber++;
                    try {
                        handleSubselectSelectClauses(subselectStreamNumber, statementContext, subselect,
                            streamTypeService.getEventTypes()[0], streamTypeService.getStreamNames()[0], streamTypeService.getStreamNames()[0],
                            taggedEventTypes, arrayEventTypes);
                    }
                    catch (ExprValidationException ex) {
                        throw new ExprValidationException("Failed to validate " + EPStatementStartMethodHelperSubselect.getSubqueryInfoText(count, subselect) + ": " + ex.getMessage(), ex);
                    }
                }
            }

            ExprNode validated = ExprNodeUtility.getValidatedSubtree(exprNodeOrigin, node, validationContext);
            validatedNodes.add(validated);

            if ((validated.getExprEvaluator().getType() != Boolean.class) && ((validated.getExprEvaluator().getType() != boolean.class)))
            {
                throw new ExprValidationException("Filter expression not returning a boolean value: '" + ExprNodeUtility.toExpressionStringMinPrecedenceSafe(validated) + "'");
            }
        }

        return validatedNodes;
    }

    private static void handleSubselectSelectClauses(int subselectStreamNumber, StatementContext statementContext, ExprSubselectNode subselect, EventType outerEventType, String outerEventTypeName, String outerStreamName,
            LinkedHashMap<String, Pair<EventType, String>> taggedEventTypes, LinkedHashMap<String, Pair<EventType, String>> arrayEventTypes)
        throws ExprValidationException {

        StatementSpecCompiled statementSpec = subselect.getStatementSpecCompiled();
        StreamSpecCompiled filterStreamSpec = statementSpec.getStreamSpecs()[0];

        ViewFactoryChain viewFactoryChain;
        String subselecteventTypeName;

        // construct view factory chain
        try {
            if (statementSpec.getStreamSpecs()[0] instanceof FilterStreamSpecCompiled)
            {
                FilterStreamSpecCompiled filterStreamSpecCompiled = (FilterStreamSpecCompiled) statementSpec.getStreamSpecs()[0];
                subselecteventTypeName = filterStreamSpecCompiled.getFilterSpec().getFilterForEventTypeName();

                // A child view is required to limit the stream
                if (filterStreamSpec.getViewSpecs().length == 0)
                {
                    throw new ExprValidationException("Subqueries require one or more views to limit the stream, consider declaring a length or time window");
                }

                // Register filter, create view factories
                viewFactoryChain = statementContext.getViewService().createFactories(subselectStreamNumber, filterStreamSpecCompiled.getFilterSpec().getResultEventType(), filterStreamSpec.getViewSpecs(), filterStreamSpec.getOptions(), statementContext);
                subselect.setRawEventType(viewFactoryChain.getEventType());
            }
            else
            {
                NamedWindowConsumerStreamSpec namedSpec = (NamedWindowConsumerStreamSpec) statementSpec.getStreamSpecs()[0];
                NamedWindowProcessor processor = statementContext.getNamedWindowService().getProcessor(namedSpec.getWindowName());
                viewFactoryChain = statementContext.getViewService().createFactories(0, processor.getNamedWindowType(), namedSpec.getViewSpecs(), namedSpec.getOptions(), statementContext);
                subselecteventTypeName = namedSpec.getWindowName();
                EPLValidationUtil.validateContextName(false, processor.getNamedWindowName(), processor.getContextName(), statementContext.getContextName(), true);
            }
        }
        catch (ViewProcessingException ex) {
            throw new ExprValidationException("Error validating subexpression: " + ex.getMessage(), ex);
        }

        // the final event type
        EventType eventType = viewFactoryChain.getEventType();

        // determine a stream name unless one was supplied
        String subexpressionStreamName = filterStreamSpec.getOptionalStreamName();
        if (subexpressionStreamName == null)
        {
            subexpressionStreamName = "$subselect_" + subselectStreamNumber;
        }

        // Named windows don't allow data views
        if (filterStreamSpec instanceof NamedWindowConsumerStreamSpec)
        {
            EPStatementStartMethodHelperValidate.validateNoDataWindowOnNamedWindow(viewFactoryChain.getViewFactoryChain());
        }

        // Streams event types are the original stream types with the stream zero the subselect stream
        LinkedHashMap<String, Pair<EventType, String>> namesAndTypes = new LinkedHashMap<String, Pair<EventType, String>>();
        namesAndTypes.put(subexpressionStreamName, new Pair<EventType, String>(eventType, subselecteventTypeName));
        namesAndTypes.put(outerStreamName, new Pair<EventType, String>(outerEventType, outerEventTypeName));
        if (taggedEventTypes != null) {
            for (Map.Entry<String, Pair<EventType, String>> entry : taggedEventTypes.entrySet()) {
                namesAndTypes.put(entry.getKey(), new Pair<EventType, String>(entry.getValue().getFirst(), entry.getValue().getSecond()));
            }
        }
        if (arrayEventTypes != null) {
            for (Map.Entry<String, Pair<EventType, String>> entry : arrayEventTypes.entrySet()) {
                namesAndTypes.put(entry.getKey(), new Pair<EventType, String>(entry.getValue().getFirst(), entry.getValue().getSecond()));
            }
        }
        StreamTypeService subselectTypeService = new StreamTypeServiceImpl(namesAndTypes, statementContext.getEngineURI(), true, true);
        ViewResourceDelegateUnverified viewResourceDelegateSubselect = new ViewResourceDelegateUnverified();
        subselect.setFilterSubqueryStreamTypes(subselectTypeService);

        // Validate select expression
        SelectClauseSpecCompiled selectClauseSpec = subselect.getStatementSpecCompiled().getSelectClauseSpec();
        if (selectClauseSpec.getSelectExprList().length > 0)
        {
            if (selectClauseSpec.getSelectExprList().length > 1) {
                throw new ExprValidationException("Subquery multi-column select is not allowed in this context.");
            }

            SelectClauseElementCompiled element = selectClauseSpec.getSelectExprList()[0];
            if (element instanceof SelectClauseExprCompiledSpec)
            {
                // validate
                SelectClauseExprCompiledSpec compiled = (SelectClauseExprCompiledSpec) element;
                ExprNode selectExpression = compiled.getSelectExpression();
                ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(statementContext, false);
                ExprValidationContext validationContext = new ExprValidationContext(subselectTypeService, statementContext.getMethodResolutionService(), viewResourceDelegateSubselect, statementContext.getSchedulingService(), statementContext.getVariableService(), statementContext.getTableService(), evaluatorContextStmt, statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, false);
                selectExpression = ExprNodeUtility.getValidatedSubtree(ExprNodeOrigin.SUBQUERYSELECT, selectExpression, validationContext);
                subselect.setSelectClause(new ExprNode[] {selectExpression});
                subselect.setSelectAsNames(new String[] {compiled.getAssignedName()});

                // handle aggregation
                List<ExprAggregateNode> aggExprNodes = new LinkedList<ExprAggregateNode>();
                ExprAggregateNodeUtil.getAggregatesBottomUp(selectExpression, aggExprNodes);
                if (aggExprNodes.size() > 0)
                {
                    // Other stream properties, if there is aggregation, cannot be under aggregation.
                    for (ExprAggregateNode aggNode : aggExprNodes)
                    {
                        List<Pair<Integer, String>> propertiesNodesAggregated = ExprNodeUtility.getExpressionProperties(aggNode, true);
                        for (Pair<Integer, String> pair : propertiesNodesAggregated)
                        {
                            if (pair.getFirst() != 0)
                            {
                                throw new ExprValidationException("Subselect aggregation function cannot aggregate across correlated properties");
                            }
                        }
                    }

                    // This stream (stream 0) properties must either all be under aggregation, or all not be.
                    List<Pair<Integer, String>> propertiesNotAggregated = ExprNodeUtility.getExpressionProperties(selectExpression, false);
                    for (Pair<Integer, String> pair : propertiesNotAggregated)
                    {
                        if (pair.getFirst() == 0)
                        {
                            throw new ExprValidationException("Subselect properties must all be within aggregation functions");
                        }
                    }
                }
            }
        }
    }
}

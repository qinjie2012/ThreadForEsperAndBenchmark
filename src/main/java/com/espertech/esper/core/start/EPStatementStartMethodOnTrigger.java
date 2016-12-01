/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.core.start;

import com.espertech.esper.client.EventType;
import com.espertech.esper.client.VariableValueException;
import com.espertech.esper.client.soda.StreamSelector;
import com.espertech.esper.collection.Pair;
import com.espertech.esper.core.context.activator.ViewableActivator;
import com.espertech.esper.core.context.activator.ViewableActivatorNamedWindow;
import com.espertech.esper.core.context.factory.*;
import com.espertech.esper.core.context.mgr.ContextManagedStatementOnTriggerDesc;
import com.espertech.esper.core.context.stmt.*;
import com.espertech.esper.core.context.subselect.SubSelectActivationCollection;
import com.espertech.esper.core.context.subselect.SubSelectStrategyCollection;
import com.espertech.esper.core.context.subselect.SubSelectStrategyFactoryDesc;
import com.espertech.esper.core.context.subselect.SubSelectStrategyHolder;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.context.util.ContextMergeViewForwarding;
import com.espertech.esper.core.context.util.ContextPropertyRegistry;
import com.espertech.esper.core.service.*;
import com.espertech.esper.core.service.resource.StatementResourceHolder;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.core.ResultSetProcessorFactoryDesc;
import com.espertech.esper.epl.core.ResultSetProcessorFactoryFactory;
import com.espertech.esper.epl.core.StreamTypeService;
import com.espertech.esper.epl.core.StreamTypeServiceImpl;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.expression.prev.ExprPreviousEvalStrategy;
import com.espertech.esper.epl.expression.prev.ExprPreviousNode;
import com.espertech.esper.epl.expression.prior.ExprPriorEvalStrategy;
import com.espertech.esper.epl.expression.prior.ExprPriorNode;
import com.espertech.esper.epl.expression.subquery.ExprSubselectNode;
import com.espertech.esper.epl.expression.table.ExprTableAccessEvalStrategy;
import com.espertech.esper.epl.expression.table.ExprTableAccessNode;
import com.espertech.esper.epl.metric.StatementMetricHandle;
import com.espertech.esper.epl.named.NamedWindowOnExprFactory;
import com.espertech.esper.epl.named.NamedWindowOnExprFactoryFactory;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.table.mgmt.TableService;
import com.espertech.esper.epl.table.onaction.TableOnViewFactory;
import com.espertech.esper.epl.table.onaction.TableOnViewFactoryFactory;
import com.espertech.esper.epl.variable.OnSetVariableViewFactory;
import com.espertech.esper.epl.view.OutputProcessViewFactory;
import com.espertech.esper.epl.view.OutputProcessViewFactoryFactory;
import com.espertech.esper.event.EventTypeMetadata;
import com.espertech.esper.event.map.MapEventType;
import com.espertech.esper.metrics.instrumentation.InstrumentationAgent;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.pattern.EvalRootFactoryNode;
import com.espertech.esper.pattern.PatternContext;
import com.espertech.esper.util.StopCallback;
import com.espertech.esper.util.UuidGenerator;
import com.espertech.esper.view.ViewProcessingException;
import com.espertech.esper.view.Viewable;

import java.util.*;

/**
 * Starts and provides the stop method for EPL statements.
 */
public class EPStatementStartMethodOnTrigger extends EPStatementStartMethodBase
{
    public static final String INITIAL_VALUE_STREAM_NAME = "initial";

    public EPStatementStartMethodOnTrigger(StatementSpecCompiled statementSpec) {
        super(statementSpec);
    }

    public EPStatementStartResult startInternal(final EPServicesContext services, final StatementContext statementContext, boolean isNewStatement, boolean isRecoveringStatement, boolean isRecoveringResilient) throws ExprValidationException, ViewProcessingException {
        // define stop and destroy
        final List<StopCallback> stopCallbacks = new LinkedList<StopCallback>();
        EPStatementDestroyCallbackList destroyCallbacks = new EPStatementDestroyCallbackList();

        // determine context
        final String contextName = statementSpec.getOptionalContextName();
        final ContextPropertyRegistry contextPropertyRegistry = (contextName != null) ? services.getContextManagementService().getContextDescriptor(contextName).getContextPropertyRegistry() : null;

        // create subselect information
        SubSelectActivationCollection subSelectStreamDesc = EPStatementStartMethodHelperSubselect.createSubSelectActivation(services, statementSpec, statementContext, destroyCallbacks);

        // obtain activator
        final StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[0];
        ActivatorResult activatorResult;
        StreamSelector optionalStreamSelector = null;
        if (streamSpec instanceof FilterStreamSpecCompiled) {
            FilterStreamSpecCompiled filterStreamSpec = (FilterStreamSpecCompiled) streamSpec;
            activatorResult = activatorFilter(statementContext, services, filterStreamSpec);
        }
        else if (streamSpec instanceof PatternStreamSpecCompiled) {
            PatternStreamSpecCompiled patternStreamSpec = (PatternStreamSpecCompiled) streamSpec;
            activatorResult = activatorPattern(statementContext, services, patternStreamSpec);
        }
        else if (streamSpec instanceof NamedWindowConsumerStreamSpec) {
            NamedWindowConsumerStreamSpec namedSpec = (NamedWindowConsumerStreamSpec) streamSpec;
            activatorResult = activatorNamedWindow(services, namedSpec);
        }
        else if (streamSpec instanceof TableQueryStreamSpec) {
            throw new ExprValidationException("Tables cannot be used in an on-action statement triggering stream");
        }
        else {
            throw new ExprValidationException("Unknown stream specification type: " + streamSpec);
        }

        // context-factory creation
        //
        // handle on-merge for table
        ContextFactoryResult contextFactoryResult;
        TableMetadata tableMetadata = null;
        if (statementSpec.getOnTriggerDesc() instanceof OnTriggerWindowDesc) {
            OnTriggerWindowDesc onTriggerDesc = (OnTriggerWindowDesc) statementSpec.getOnTriggerDesc();
            tableMetadata = services.getTableService().getTableMetadata(onTriggerDesc.getWindowName());
            if (tableMetadata != null) {
                contextFactoryResult = handleContextFactoryOnTriggerTable(statementContext, services, onTriggerDesc, contextName, streamSpec, activatorResult, contextPropertyRegistry, subSelectStreamDesc);
            }
            else if (services.getNamedWindowService().getProcessor(onTriggerDesc.getWindowName()) != null) {
                contextFactoryResult = handleContextFactoryOnTriggerNamedWindow(services, statementContext, onTriggerDesc, contextName, streamSpec, contextPropertyRegistry, subSelectStreamDesc, activatorResult, optionalStreamSelector);
            }
            else {
                throw new ExprValidationException("A named window or variable by name '" + onTriggerDesc.getWindowName() + "' does not exist");
            }
        }
        // variable assignments
        else if (statementSpec.getOnTriggerDesc() instanceof OnTriggerSetDesc) {
            OnTriggerSetDesc desc = (OnTriggerSetDesc) statementSpec.getOnTriggerDesc();
            contextFactoryResult = handleContextFactorySetVariable(statementSpec, statementContext, services, desc, streamSpec, subSelectStreamDesc, contextPropertyRegistry, activatorResult);
        }
        // split-stream use case
        else {
            OnTriggerSplitStreamDesc desc = (OnTriggerSplitStreamDesc) statementSpec.getOnTriggerDesc();
            contextFactoryResult = handleContextFactorySplitStream(statementSpec, statementContext, services, desc, streamSpec, contextPropertyRegistry, subSelectStreamDesc, activatorResult);
        }
        EventType resultEventType = contextFactoryResult.getResultSetProcessorPrototype() == null ? null : contextFactoryResult.getResultSetProcessorPrototype().getResultSetProcessorFactory().getResultEventType();

        // perform start of hook-up to start
        Viewable finalViewable;
        EPStatementStopMethod stopStatementMethod;
        Map<ExprSubselectNode, SubSelectStrategyHolder> subselectStrategyInstances;
        Map<ExprTableAccessNode, ExprTableAccessEvalStrategy> tableAccessStrategyInstances;
        AggregationService aggregationService;

        // add cleanup for table metadata, if required
        if (tableMetadata != null) {
            destroyCallbacks.addCallback(new EPStatementDestroyCallbackTableIdxRef(services.getTableService(), tableMetadata, statementContext.getStatementName()));
            destroyCallbacks.addCallback(new EPStatementDestroyCallbackTableUpdStr(services.getTableService(), tableMetadata, statementContext.getStatementName()));
        }

        // With context - delegate instantiation to context
        final EPStatementStopMethod stopMethod = new EPStatementStopMethodImpl(statementContext, stopCallbacks);
        if (statementSpec.getOptionalContextName() != null) {

            // use statement-wide agent-instance-specific aggregation service
            aggregationService = statementContext.getStatementAgentInstanceRegistry().getAgentInstanceAggregationService();

            // use statement-wide agent-instance-specific subselects
            AIRegistryExpr aiRegistryExpr = statementContext.getStatementAgentInstanceRegistry().getAgentInstanceExprService();
            subselectStrategyInstances = new HashMap<ExprSubselectNode, SubSelectStrategyHolder>();
            for (Map.Entry<ExprSubselectNode, SubSelectStrategyFactoryDesc> entry : contextFactoryResult.subSelectStrategyCollection.getSubqueries().entrySet()) {
                AIRegistrySubselect specificService = aiRegistryExpr.allocateSubselect(entry.getKey());
                entry.getKey().setStrategy(specificService);

                Map<ExprPriorNode, ExprPriorEvalStrategy> subselectPriorStrategies = new HashMap<ExprPriorNode, ExprPriorEvalStrategy>();
                for (ExprPriorNode subselectPrior : entry.getValue().getPriorNodesList()) {
                    AIRegistryPrior specificSubselectPriorService = aiRegistryExpr.allocatePrior(subselectPrior);
                    subselectPriorStrategies.put(subselectPrior, specificSubselectPriorService);
                }

                Map<ExprPreviousNode, ExprPreviousEvalStrategy> subselectPreviousStrategies = new HashMap<ExprPreviousNode, ExprPreviousEvalStrategy>();
                for (ExprPreviousNode subselectPrevious : entry.getValue().getPrevNodesList()) {
                    AIRegistryPrevious specificSubselectPreviousService = aiRegistryExpr.allocatePrevious(subselectPrevious);
                    subselectPreviousStrategies.put(subselectPrevious, specificSubselectPreviousService);
                }

                AIRegistryAggregation subselectAggregation = aiRegistryExpr.allocateSubselectAggregation(entry.getKey());
                subselectStrategyInstances.put(entry.getKey(), new SubSelectStrategyHolder(specificService, subselectAggregation, subselectPriorStrategies, subselectPreviousStrategies, null, null));
            }

            // use statement-wide agent-instance-specific tables
            tableAccessStrategyInstances = new HashMap<ExprTableAccessNode, ExprTableAccessEvalStrategy>();
            if (statementSpec.getTableNodes() != null) {
                for (ExprTableAccessNode tableNode : statementSpec.getTableNodes()) {
                    AIRegistryTableAccess specificService = aiRegistryExpr.allocateTableAccess(tableNode);
                    tableAccessStrategyInstances.put(tableNode, specificService);
                }
            }

            ContextMergeViewForwarding mergeView = new ContextMergeViewForwarding(resultEventType);
            finalViewable = mergeView;

            ContextManagedStatementOnTriggerDesc statement = new ContextManagedStatementOnTriggerDesc(statementSpec, statementContext, mergeView, contextFactoryResult.getContextFactory());
            services.getContextManagementService().addStatement(contextName, statement, isRecoveringResilient);
            stopStatementMethod = new EPStatementStopMethod(){
                public void stop()
                {
                    services.getContextManagementService().stoppedStatement(contextName, statementContext.getStatementName(), statementContext.getStatementId());
                    stopMethod.stop();
                }
            };

            destroyCallbacks.addCallback(new EPStatementDestroyCallbackContext(services.getContextManagementService(), contextName, statementContext.getStatementName(), statementContext.getStatementId()));
        }
        // Without context - start here
        else {
            AgentInstanceContext agentInstanceContext = getDefaultAgentInstanceContext(statementContext);
            final StatementAgentInstanceFactoryOnTriggerResult resultOfStart = contextFactoryResult.getContextFactory().newContext(agentInstanceContext, isRecoveringResilient);
            finalViewable = resultOfStart.getFinalView();
            stopStatementMethod = new EPStatementStopMethod() {
                public void stop() {
                    resultOfStart.getStopCallback().stop();
                    stopMethod.stop();
                }
            };
            aggregationService = resultOfStart.getOptionalAggegationService();
            subselectStrategyInstances = resultOfStart.getSubselectStrategies();
            tableAccessStrategyInstances = resultOfStart.getTableAccessEvalStrategies();

            if (statementContext.getStatementExtensionServicesContext() != null && statementContext.getStatementExtensionServicesContext().getStmtResources() != null) {
                StatementResourceHolder holder = statementContext.getStatementExtensionServicesContext().extractStatementResourceHolder(resultOfStart);
                statementContext.getStatementExtensionServicesContext().getStmtResources().setUnpartitioned(holder);
                statementContext.getStatementExtensionServicesContext().postProcessStart(resultOfStart, isRecoveringResilient);
            }
        }

        // initialize aggregation expression nodes
        if (contextFactoryResult.getResultSetProcessorPrototype() != null && contextFactoryResult.getResultSetProcessorPrototype().getAggregationServiceFactoryDesc() != null) {
            EPStatementStartMethodHelperAssignExpr.assignAggregations(aggregationService, contextFactoryResult.getResultSetProcessorPrototype().getAggregationServiceFactoryDesc().getExpressions());
        }

        // assign subquery nodes
        EPStatementStartMethodHelperAssignExpr.assignSubqueryStrategies(contextFactoryResult.getSubSelectStrategyCollection(), subselectStrategyInstances);

        // assign tables
        EPStatementStartMethodHelperAssignExpr.assignTableAccessStrategies(tableAccessStrategyInstances);

        return new EPStatementStartResult(finalViewable, stopStatementMethod, destroyCallbacks);
    }

    private ActivatorResult activatorNamedWindow(EPServicesContext services, NamedWindowConsumerStreamSpec namedSpec)
            throws ExprValidationException
    {
        NamedWindowProcessor processor = services.getNamedWindowService().getProcessor(namedSpec.getWindowName());
        if (processor == null) {
            throw new ExprValidationException("A named window by name '" + namedSpec.getWindowName() + "' does not exist");
        }
        String triggerEventTypeName = namedSpec.getWindowName();
        ViewableActivator activator = services.getViewableActivatorFactory().createNamedWindow(processor, namedSpec.getFilterExpressions(), namedSpec.getOptPropertyEvaluator());
        EventType activatorResultEventType = processor.getNamedWindowType();
        if (namedSpec.getOptPropertyEvaluator() != null) {
            activatorResultEventType = namedSpec.getOptPropertyEvaluator().getFragmentEventType();
        }
        return new ActivatorResult(activator, triggerEventTypeName, activatorResultEventType);
    }

    private ActivatorResult activatorPattern(StatementContext statementContext, EPServicesContext services, PatternStreamSpecCompiled patternStreamSpec) {
        boolean usedByChildViews = patternStreamSpec.getViewSpecs().length > 0 || (statementSpec.getInsertIntoDesc() != null);
        String patternTypeName = statementContext.getStatementId() + "_patternon";
        final EventType eventType = services.getEventAdapterService().createSemiAnonymousMapType(patternTypeName, patternStreamSpec.getTaggedEventTypes(), patternStreamSpec.getArrayEventTypes(), usedByChildViews);

        EvalRootFactoryNode rootNode = services.getPatternNodeFactory().makeRootNode(patternStreamSpec.getEvalFactoryNode());
        PatternContext patternContext = statementContext.getPatternContextFactory().createContext(statementContext, 0, rootNode, patternStreamSpec.getMatchedEventMapMeta(), true);
        ViewableActivator activator = services.getViewableActivatorFactory().createPattern(patternContext, rootNode, eventType, EPStatementStartMethodHelperUtil.isConsumingFilters(patternStreamSpec.getEvalFactoryNode()), false, false, false);
        return new ActivatorResult(activator, null, eventType);
    }

    private ActivatorResult activatorFilter(StatementContext statementContext, EPServicesContext services, FilterStreamSpecCompiled filterStreamSpec) {
        String triggerEventTypeName = filterStreamSpec.getFilterSpec().getFilterForEventTypeName();
        InstrumentationAgent instrumentationAgentOnTrigger = null;
        if (InstrumentationHelper.ENABLED) {
            final String eventTypeName = filterStreamSpec.getFilterSpec().getFilterForEventType().getName();
            instrumentationAgentOnTrigger = new InstrumentationAgent() {
                public void indicateQ() {
                    InstrumentationHelper.get().qFilterActivationOnTrigger(eventTypeName);
                }
                public void indicateA() {
                    InstrumentationHelper.get().aFilterActivationOnTrigger();
                }
            };
        }
        ViewableActivator activator = services.getViewableActivatorFactory().createFilterProxy(services, filterStreamSpec.getFilterSpec(), statementContext.getAnnotations(), false, instrumentationAgentOnTrigger, false);
        EventType activatorResultEventType = filterStreamSpec.getFilterSpec().getResultEventType();
        return new ActivatorResult(activator, triggerEventTypeName, activatorResultEventType);
    }

    private ContextFactoryResult handleContextFactorySetVariable(StatementSpecCompiled statementSpec, StatementContext statementContext, EPServicesContext services, OnTriggerSetDesc desc, StreamSpecCompiled streamSpec, SubSelectActivationCollection subSelectStreamDesc, ContextPropertyRegistry contextPropertyRegistry, ActivatorResult activatorResult)
            throws ExprValidationException
    {
        StreamTypeService typeService = new StreamTypeServiceImpl(new EventType[] {activatorResult.activatorResultEventType}, new String[] {streamSpec.getOptionalStreamName()}, new boolean[] {true}, services.getEngineURI(), false);
        ExprValidationContext validationContext = new ExprValidationContext(typeService, statementContext.getMethodResolutionService(), null, statementContext.getSchedulingService(), statementContext.getVariableService(), statementContext.getTableService(), getDefaultAgentInstanceContext(statementContext), statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, false);

        // Materialize sub-select views
        SubSelectStrategyCollection subSelectStrategyCollection = EPStatementStartMethodHelperSubselect.planSubSelect(services, statementContext, isQueryPlanLogging(services), subSelectStreamDesc, new String[]{streamSpec.getOptionalStreamName()}, new EventType[]{activatorResult.activatorResultEventType}, new String[]{activatorResult.triggerEventTypeName}, statementSpec.getDeclaredExpressions(), contextPropertyRegistry);

        for (OnTriggerSetAssignment assignment : desc.getAssignments()) {
            ExprNode validated = ExprNodeUtility.getValidatedAssignment(assignment, validationContext);
            assignment.setExpression(validated);
        }

        OnSetVariableViewFactory onSetVariableViewFactory;
        try {
            ExprEvaluatorContextStatement exprEvaluatorContext = new ExprEvaluatorContextStatement(statementContext, false);
            onSetVariableViewFactory = new OnSetVariableViewFactory(statementContext.getStatementId(), desc, statementContext.getEventAdapterService(), statementContext.getVariableService(), statementContext.getStatementResultService(), exprEvaluatorContext);
        }
        catch (VariableValueException ex) {
            throw new ExprValidationException("Error in variable assignment: " + ex.getMessage(), ex);
        }

        EventType outputEventType = onSetVariableViewFactory.getEventType();

        // handle output format
        StatementSpecCompiled defaultSelectAllSpec = new StatementSpecCompiled();
        defaultSelectAllSpec.getSelectClauseSpec().setSelectExprList(new SelectClauseElementWildcard());
        StreamTypeService streamTypeService = new StreamTypeServiceImpl(new EventType[] {outputEventType}, new String[] {"trigger_stream"}, new boolean[] {true}, services.getEngineURI(), false);
        ResultSetProcessorFactoryDesc outputResultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(defaultSelectAllSpec, statementContext, streamTypeService, null, new boolean[0], true, contextPropertyRegistry, null, services.getConfigSnapshot());

        OutputProcessViewFactory outputViewFactory = OutputProcessViewFactoryFactory.make(statementSpec, services.getInternalEventRouter(), statementContext, null, null, services.getTableService(), outputResultSetProcessorPrototype.getResultSetProcessorFactory().getResultSetProcessorType());
        StatementAgentInstanceFactoryOnTriggerSetVariable contextFactory = new StatementAgentInstanceFactoryOnTriggerSetVariable(statementContext, statementSpec, services, activatorResult.activator, subSelectStrategyCollection, onSetVariableViewFactory, outputResultSetProcessorPrototype, outputViewFactory);
        return new ContextFactoryResult(contextFactory, subSelectStrategyCollection, null);
    }

    private ContextFactoryResult handleContextFactorySplitStream(StatementSpecCompiled statementSpec, StatementContext statementContext, EPServicesContext services, OnTriggerSplitStreamDesc desc, StreamSpecCompiled streamSpec, ContextPropertyRegistry contextPropertyRegistry, SubSelectActivationCollection subSelectStreamDesc, ActivatorResult activatorResult)
            throws ExprValidationException
    {
        String streamName = streamSpec.getOptionalStreamName();
        if (streamName == null)
        {
            streamName = "stream_0";
        }
        StreamTypeService typeService = new StreamTypeServiceImpl(new EventType[] {activatorResult.activatorResultEventType}, new String[] {streamName}, new boolean[] {true}, services.getEngineURI(), false);
        if (statementSpec.getInsertIntoDesc() == null)
        {
            throw new ExprValidationException("Required insert-into clause is not provided, the clause is required for split-stream syntax");
        }
        if ((statementSpec.getGroupByExpressions() != null && statementSpec.getGroupByExpressions().getGroupByNodes().length > 0) || (statementSpec.getHavingExprRootNode() != null) || (statementSpec.getOrderByList().length > 0))
        {
            throw new ExprValidationException("A group-by clause, having-clause or order-by clause is not allowed for the split stream syntax");
        }

        // Materialize sub-select views
        SubSelectStrategyCollection subSelectStrategyCollection = EPStatementStartMethodHelperSubselect.planSubSelect(services, statementContext, isQueryPlanLogging(services), subSelectStreamDesc, new String[]{streamSpec.getOptionalStreamName()}, new EventType[]{activatorResult.activatorResultEventType}, new String[]{activatorResult.triggerEventTypeName}, statementSpec.getDeclaredExpressions(), contextPropertyRegistry);

        EPStatementStartMethodHelperValidate.validateNodes(statementSpec, statementContext, typeService, null);

        ResultSetProcessorFactoryDesc[] processorFactories = new ResultSetProcessorFactoryDesc[desc.getSplitStreams().size() + 1];
        ExprNode[] whereClauses = new ExprNode[desc.getSplitStreams().size() + 1];
        processorFactories[0] = ResultSetProcessorFactoryFactory.getProcessorPrototype(
                statementSpec, statementContext, typeService, null, new boolean[0], false, contextPropertyRegistry, null, services.getConfigSnapshot());
        whereClauses[0] = statementSpec.getFilterRootNode();
        boolean[] isNamedWindowInsert = new boolean[desc.getSplitStreams().size() + 1];
        isNamedWindowInsert[0] = false;
        String[] insertIntoTableNames = new String[desc.getSplitStreams().size() + 1];
        insertIntoTableNames[0] = getOptionalInsertIntoTableName(statementSpec.getInsertIntoDesc(), services.getTableService());

        int index = 1;
        for (OnTriggerSplitStream splits : desc.getSplitStreams())
        {
            StatementSpecCompiled splitSpec = new StatementSpecCompiled();
            splitSpec.setInsertIntoDesc(splits.getInsertInto());
            splitSpec.setSelectClauseSpec(StatementLifecycleSvcImpl.compileSelectAllowSubselect(splits.getSelectClause()));
            splitSpec.setFilterExprRootNode(splits.getWhereClause());
            EPStatementStartMethodHelperValidate.validateNodes(splitSpec, statementContext, typeService, null);

            processorFactories[index] = ResultSetProcessorFactoryFactory.getProcessorPrototype(
                    splitSpec, statementContext, typeService, null, new boolean[0], false, contextPropertyRegistry, null, services.getConfigSnapshot());
            whereClauses[index] = splitSpec.getFilterRootNode();
            isNamedWindowInsert[index] = statementContext.getNamedWindowService().isNamedWindow(splits.getInsertInto().getEventTypeName());
            insertIntoTableNames[index] = getOptionalInsertIntoTableName(splits.getInsertInto(), services.getTableService());

            index++;
        }

        StatementAgentInstanceFactoryOnTriggerSplitDesc splitDesc = new StatementAgentInstanceFactoryOnTriggerSplitDesc(processorFactories, whereClauses, isNamedWindowInsert);
        StatementAgentInstanceFactoryOnTriggerSplit contextFactory = new StatementAgentInstanceFactoryOnTriggerSplit(statementContext, statementSpec, services, activatorResult.activator, subSelectStrategyCollection, splitDesc, activatorResult.activatorResultEventType, insertIntoTableNames);
        return new ContextFactoryResult(contextFactory, subSelectStrategyCollection, null);
    }

    private String getOptionalInsertIntoTableName(InsertIntoDesc insertIntoDesc, TableService tableService) {
        if (insertIntoDesc == null) {
            return null;
        }
        TableMetadata tableMetadata = tableService.getTableMetadata(insertIntoDesc.getEventTypeName());
        if (tableMetadata != null) {
            return tableMetadata.getTableName();
        }
        return null;
    }

    private ContextFactoryResult handleContextFactoryOnTriggerNamedWindow(EPServicesContext services, StatementContext statementContext, OnTriggerWindowDesc onTriggerDesc, String contextName, StreamSpecCompiled streamSpec, ContextPropertyRegistry contextPropertyRegistry, SubSelectActivationCollection subSelectStreamDesc, ActivatorResult activatorResult, StreamSelector optionalStreamSelector)
            throws ExprValidationException
    {
        NamedWindowProcessor processor = services.getNamedWindowService().getProcessor(onTriggerDesc.getWindowName());

        // validate context
        validateOnExpressionContext(contextName, processor.getContextName(), "Named window '" + onTriggerDesc.getWindowName() + "'");

        EventType namedWindowType = processor.getNamedWindowType();
        services.getStatementEventTypeRefService().addReferences(statementContext.getStatementName(), new String[] {onTriggerDesc.getWindowName()});

        // validate expressions and plan subselects
        TriggerValidationPlanResult validationResult = validateOnTriggerPlanSubSelects(services, statementContext, onTriggerDesc, namedWindowType, streamSpec, activatorResult, contextPropertyRegistry, subSelectStreamDesc, null);

        InternalEventRouter routerService = null;
        boolean addToFront = false;
        String optionalInsertIntoTableName = null;
        if (statementSpec.getInsertIntoDesc() != null || onTriggerDesc instanceof OnTriggerMergeDesc) {
            routerService = services.getInternalEventRouter();
        }
        if (statementSpec.getInsertIntoDesc() != null) {
            TableMetadata tableMetadata = services.getTableService().getTableMetadata(statementSpec.getInsertIntoDesc().getEventTypeName());
            if (tableMetadata != null) {
                optionalInsertIntoTableName = tableMetadata.getTableName();
                routerService = null;
            }
            addToFront = statementContext.getNamedWindowService().isNamedWindow(statementSpec.getInsertIntoDesc().getEventTypeName());
        }
        boolean isDistinct = statementSpec.getSelectClauseSpec().isDistinct();
        EventType selectResultEventType = validationResult.resultSetProcessorPrototype.getResultSetProcessorFactory().getResultEventType();
        StatementMetricHandle createNamedWindowMetricsHandle = processor.getCreateNamedWindowMetricsHandle();

        NamedWindowOnExprFactory onExprFactory = NamedWindowOnExprFactoryFactory.make(namedWindowType, onTriggerDesc.getWindowName(), validationResult.zeroStreamAliasName,
                onTriggerDesc,
                activatorResult.activatorResultEventType, streamSpec.getOptionalStreamName(), addToFront, routerService,
                selectResultEventType,
                statementContext, createNamedWindowMetricsHandle, isDistinct, optionalStreamSelector, optionalInsertIntoTableName);

        // For on-delete/set/update/merge, create an output processor that passes on as a wildcard the underlying event
        ResultSetProcessorFactoryDesc outputResultSetProcessorPrototype = null;
        if ((statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_DELETE) ||
                (statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_UPDATE) ||
                (statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_MERGE))
        {
            StatementSpecCompiled defaultSelectAllSpec = new StatementSpecCompiled();
            defaultSelectAllSpec.getSelectClauseSpec().setSelectExprList(new SelectClauseElementWildcard());
            StreamTypeService streamTypeService = new StreamTypeServiceImpl(new EventType[] {namedWindowType}, new String[] {"trigger_stream"}, new boolean[] {true}, services.getEngineURI(), false);
            outputResultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(defaultSelectAllSpec, statementContext, streamTypeService, null, new boolean[0], true, contextPropertyRegistry, null, services.getConfigSnapshot());
        }

        EventType resultEventType = validationResult.resultSetProcessorPrototype.getResultSetProcessorFactory().getResultEventType();
        OutputProcessViewFactory outputViewFactory = OutputProcessViewFactoryFactory.make(statementSpec, services.getInternalEventRouter(), statementContext, resultEventType, null, services.getTableService(), validationResult.resultSetProcessorPrototype.getResultSetProcessorFactory().getResultSetProcessorType());

        StatementAgentInstanceFactoryOnTriggerNamedWindow contextFactory = new StatementAgentInstanceFactoryOnTriggerNamedWindow(statementContext, statementSpec, services, activatorResult.activator, validationResult.subSelectStrategyCollection, validationResult.resultSetProcessorPrototype, validationResult.validatedJoin, outputResultSetProcessorPrototype, onExprFactory, outputViewFactory, activatorResult.activatorResultEventType, processor);
        return new ContextFactoryResult(contextFactory, validationResult.subSelectStrategyCollection, validationResult.resultSetProcessorPrototype);
    }

    private TriggerValidationPlanResult validateOnTriggerPlanSubSelects(EPServicesContext services,
                                                                        StatementContext statementContext,
                                                                        OnTriggerWindowDesc onTriggerDesc,
                                                                        EventType namedWindowType,
                                                                        StreamSpecCompiled streamSpec,
                                                                        ActivatorResult activatorResult,
                                                                        ContextPropertyRegistry contextPropertyRegistry,
                                                                        SubSelectActivationCollection subSelectStreamDesc,
                                                                        String optionalTableName)
            throws ExprValidationException
    {
        String zeroStreamAliasName = onTriggerDesc.getOptionalAsName();
        if (zeroStreamAliasName == null) {
            zeroStreamAliasName = "stream_0";
        }
        String streamName = streamSpec.getOptionalStreamName();
        if (streamName == null) {
            streamName = "stream_1";
        }
        String namedWindowTypeName = onTriggerDesc.getWindowName();

        // Materialize sub-select views
        // 0 - named window stream
        // 1 - arriving stream
        // 2 - initial value before update
        SubSelectStrategyCollection subSelectStrategyCollection = EPStatementStartMethodHelperSubselect.planSubSelect(services, statementContext, isQueryPlanLogging(services), subSelectStreamDesc, new String[]{zeroStreamAliasName, streamSpec.getOptionalStreamName()}, new EventType[]{namedWindowType, activatorResult.activatorResultEventType}, new String[]{namedWindowTypeName, activatorResult.triggerEventTypeName}, statementSpec.getDeclaredExpressions(), contextPropertyRegistry);

        StreamTypeServiceImpl typeService = new StreamTypeServiceImpl(new EventType[] {namedWindowType, activatorResult.activatorResultEventType}, new String[] {zeroStreamAliasName, streamName}, new boolean[] {false, true}, services.getEngineURI(), true);

        // allow "initial" as a prefix to properties
        StreamTypeServiceImpl assignmentTypeService;
        if (zeroStreamAliasName.equals(INITIAL_VALUE_STREAM_NAME) || streamName.equals(INITIAL_VALUE_STREAM_NAME)) {
            assignmentTypeService = typeService;
        }
        else {
            assignmentTypeService = new StreamTypeServiceImpl(new EventType[] {namedWindowType, activatorResult.activatorResultEventType, namedWindowType}, new String[] {zeroStreamAliasName, streamName, INITIAL_VALUE_STREAM_NAME}, new boolean[] {false, true, true}, services.getEngineURI(), false);
            assignmentTypeService.setStreamZeroUnambigous(true);
        }

        if (onTriggerDesc instanceof OnTriggerWindowUpdateDesc) {
            OnTriggerWindowUpdateDesc updateDesc = (OnTriggerWindowUpdateDesc) onTriggerDesc;
            ExprValidationContext validationContext = new ExprValidationContext(assignmentTypeService, statementContext.getMethodResolutionService(), null, statementContext.getSchedulingService(), statementContext.getVariableService(), statementContext.getTableService(), getDefaultAgentInstanceContext(statementContext), statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, false);
            for (OnTriggerSetAssignment assignment : updateDesc.getAssignments())
            {
                ExprNode validated = ExprNodeUtility.getValidatedAssignment(assignment, validationContext);
                assignment.setExpression(validated);
                EPStatementStartMethodHelperValidate.validateNoAggregations(validated, "Aggregation functions may not be used within an on-update-clause");
            }
        }
        if (onTriggerDesc instanceof OnTriggerMergeDesc) {
            OnTriggerMergeDesc mergeDesc = (OnTriggerMergeDesc) onTriggerDesc;
            validateMergeDesc(mergeDesc, statementContext, namedWindowType, zeroStreamAliasName,  activatorResult.activatorResultEventType, streamName);
        }

        // validate join expression
        ExprNode validatedJoin = validateJoinNamedWindow(services.getEngineURI(), statementContext, ExprNodeOrigin.WHERE, statementSpec.getFilterRootNode(),
                namedWindowType, zeroStreamAliasName, namedWindowTypeName,
                activatorResult.activatorResultEventType, streamName, activatorResult.triggerEventTypeName,
                optionalTableName);

        // validate filter, output rate limiting
        EPStatementStartMethodHelperValidate.validateNodes(statementSpec, statementContext, typeService, null);

        // Construct a processor for results; for use in on-select to process selection results
        // Use a wildcard select if the select-clause is empty, such as for on-delete.
        // For on-select the select clause is not empty.
        if (statementSpec.getSelectClauseSpec().getSelectExprList().length == 0) {
            statementSpec.getSelectClauseSpec().setSelectExprList(new SelectClauseElementWildcard());
        }
        ResultSetProcessorFactoryDesc resultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(
                statementSpec, statementContext, typeService, null, new boolean[0], true, contextPropertyRegistry, null, services.getConfigSnapshot());

        return new TriggerValidationPlanResult(subSelectStrategyCollection, resultSetProcessorPrototype, validatedJoin, zeroStreamAliasName);
    }

    private void validateOnExpressionContext(String onExprContextName, String desiredContextName, String title)
            throws ExprValidationException
    {
        if (onExprContextName == null) {
            if (desiredContextName != null) {
                throw new ExprValidationException("Cannot create on-trigger expression: " + title + " was declared with context '" + desiredContextName + "', please declare the same context name");
            }
            return;
        }
        if (!onExprContextName.equals(desiredContextName)) {
            throw new ExprValidationException("Cannot create on-trigger expression: " + title + " was declared with context '" + desiredContextName + "', please use the same context instead");
        }
    }

    private ContextFactoryResult handleContextFactoryOnTriggerTable(StatementContext statementContext, EPServicesContext services, OnTriggerWindowDesc onTriggerDesc, String contextName, StreamSpecCompiled streamSpec, ActivatorResult activatorResult, ContextPropertyRegistry contextPropertyRegistry, SubSelectActivationCollection subSelectStreamDesc)
            throws ExprValidationException
    {
        TableMetadata metadata = services.getTableService().getTableMetadata(onTriggerDesc.getWindowName());

        // validate context
        validateOnExpressionContext(contextName, metadata.getContextName(), "Table '" + onTriggerDesc.getWindowName() + "'");

        InternalEventRouter routerService = null;
        if (statementSpec.getInsertIntoDesc() != null || onTriggerDesc instanceof OnTriggerMergeDesc) {
            routerService = services.getInternalEventRouter();
        }

        // validate expressions and plan subselects
        TriggerValidationPlanResult validationResult = validateOnTriggerPlanSubSelects(services, statementContext, onTriggerDesc, metadata.getInternalEventType(), streamSpec, activatorResult, contextPropertyRegistry, subSelectStreamDesc, metadata.getTableName());

        // table on-action view factory
        TableOnViewFactory onExprFactory = TableOnViewFactoryFactory.make(metadata, onTriggerDesc, activatorResult.activatorResultEventType, streamSpec.getOptionalStreamName(),
                statementContext, statementContext.getEpStatementHandle().getMetricsHandle(), false, routerService);

        // For on-delete/set/update/merge, create an output processor that passes on as a wildcard the underlying event
        ResultSetProcessorFactoryDesc outputResultSetProcessorPrototype = null;
        if ((statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_DELETE) ||
                (statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_UPDATE) ||
                (statementSpec.getOnTriggerDesc().getOnTriggerType() == OnTriggerType.ON_MERGE))
        {
            StatementSpecCompiled defaultSelectAllSpec = new StatementSpecCompiled();
            defaultSelectAllSpec.getSelectClauseSpec().setSelectExprList(new SelectClauseElementWildcard());
            // we'll be expecting public-type events as there is no copy op
            StreamTypeService streamTypeService = new StreamTypeServiceImpl(new EventType[] {metadata.getPublicEventType()}, new String[] {"trigger_stream"}, new boolean[] {true}, services.getEngineURI(), false);
            outputResultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(defaultSelectAllSpec, statementContext, streamTypeService, null, new boolean[0], true, contextPropertyRegistry, null, services.getConfigSnapshot());
        }

        EventType resultEventType = validationResult.resultSetProcessorPrototype.getResultSetProcessorFactory().getResultEventType();
        OutputProcessViewFactory outputViewFactory = OutputProcessViewFactoryFactory.make(statementSpec, services.getInternalEventRouter(), statementContext, resultEventType, null, services.getTableService(), validationResult.resultSetProcessorPrototype.getResultSetProcessorFactory().getResultSetProcessorType());

        StatementAgentInstanceFactoryOnTriggerTable contextFactory = new StatementAgentInstanceFactoryOnTriggerTable(statementContext, statementSpec, services, activatorResult.activator, validationResult.getSubSelectStrategyCollection(), validationResult.getResultSetProcessorPrototype(), validationResult.validatedJoin, onExprFactory, activatorResult.activatorResultEventType, metadata, outputResultSetProcessorPrototype, outputViewFactory);

        return new ContextFactoryResult(contextFactory, validationResult.getSubSelectStrategyCollection(), validationResult.resultSetProcessorPrototype);
    }

    private void validateMergeDesc(OnTriggerMergeDesc mergeDesc, StatementContext statementContext, EventType namedWindowType, String namedWindowName, EventType triggerStreamType, String triggerStreamName)
        throws ExprValidationException
    {
        String exprNodeErrorMessage = "Aggregation functions may not be used within an merge-clause";
        ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(statementContext, false);

        for (OnTriggerMergeMatched matchedItem : mergeDesc.getItems()) {

            EventType dummyTypeNoProperties = new MapEventType(EventTypeMetadata.createAnonymous("merge_named_window_insert"), "merge_named_window_insert", 0, null, Collections.<String, Object>emptyMap(), null, null, null);
            StreamTypeServiceImpl twoStreamTypeSvc = new StreamTypeServiceImpl(new EventType[] {namedWindowType, triggerStreamType},
                    new String[] {namedWindowName, triggerStreamName}, new boolean[] {true, true}, statementContext.getEngineURI(), true);
            StreamTypeService insertOnlyTypeSvc = new StreamTypeServiceImpl(new EventType[] {dummyTypeNoProperties, triggerStreamType},
                    new String[] {UuidGenerator.generate(), triggerStreamName}, new boolean[] {true, true}, statementContext.getEngineURI(), true);

            // we may provide an additional stream "initial" for the prior value, unless already defined
            StreamTypeServiceImpl assignmentStreamTypeSvc;
            if (namedWindowName.equals(INITIAL_VALUE_STREAM_NAME) || triggerStreamName.equals(INITIAL_VALUE_STREAM_NAME)) {
                assignmentStreamTypeSvc = twoStreamTypeSvc;
            }
            else {
                assignmentStreamTypeSvc = new StreamTypeServiceImpl(new EventType[] {namedWindowType, triggerStreamType, namedWindowType},
                        new String[] {namedWindowName, triggerStreamName, INITIAL_VALUE_STREAM_NAME}, new boolean[] {true, true, true}, statementContext.getEngineURI(), false);
                assignmentStreamTypeSvc.setStreamZeroUnambigous(true);
            }

            if (matchedItem.getOptionalMatchCond() != null) {
                StreamTypeService matchValidStreams = matchedItem.isMatchedUnmatched() ? twoStreamTypeSvc : insertOnlyTypeSvc;
                matchedItem.setOptionalMatchCond(EPStatementStartMethodHelperValidate.validateExprNoAgg(ExprNodeOrigin.MERGEMATCHCOND, matchedItem.getOptionalMatchCond(), matchValidStreams, statementContext, evaluatorContextStmt, exprNodeErrorMessage, true));
                if (!matchedItem.isMatchedUnmatched()) {
                    EPStatementStartMethodHelperValidate.validateSubqueryExcludeOuterStream(matchedItem.getOptionalMatchCond());
                }
            }

            for (OnTriggerMergeAction item : matchedItem.getActions()) {
                if (item instanceof OnTriggerMergeActionDelete) {
                    OnTriggerMergeActionDelete delete = (OnTriggerMergeActionDelete) item;
                    if (delete.getOptionalWhereClause() != null) {
                        delete.setOptionalWhereClause(EPStatementStartMethodHelperValidate.validateExprNoAgg(ExprNodeOrigin.MERGEMATCHWHERE, delete.getOptionalWhereClause(), twoStreamTypeSvc, statementContext, evaluatorContextStmt, exprNodeErrorMessage, true));
                    }
                }
                else if (item instanceof OnTriggerMergeActionUpdate) {
                    OnTriggerMergeActionUpdate update = (OnTriggerMergeActionUpdate) item;
                    if (update.getOptionalWhereClause() != null) {
                        update.setOptionalWhereClause(EPStatementStartMethodHelperValidate.validateExprNoAgg(ExprNodeOrigin.MERGEMATCHWHERE, update.getOptionalWhereClause(), twoStreamTypeSvc, statementContext, evaluatorContextStmt, exprNodeErrorMessage, true));
                    }
                    for (OnTriggerSetAssignment assignment : update.getAssignments())
                    {
                        assignment.setExpression(EPStatementStartMethodHelperValidate.validateExprNoAgg(ExprNodeOrigin.UPDATEASSIGN, assignment.getExpression(), assignmentStreamTypeSvc, statementContext, evaluatorContextStmt, exprNodeErrorMessage, true));
                    }
                }
                else if (item instanceof OnTriggerMergeActionInsert) {
                    OnTriggerMergeActionInsert insert = (OnTriggerMergeActionInsert) item;

                    StreamTypeService insertTypeSvc;
                    if (insert.getOptionalStreamName() == null || insert.getOptionalStreamName().equals(namedWindowName)) {
                        insertTypeSvc = insertOnlyTypeSvc;
                    }
                    else {
                        insertTypeSvc = twoStreamTypeSvc;
                    }

                    List<SelectClauseElementCompiled> compiledSelect = new ArrayList<SelectClauseElementCompiled>();
                    if (insert.getOptionalWhereClause() != null) {
                        insert.setOptionalWhereClause(EPStatementStartMethodHelperValidate.validateExprNoAgg(ExprNodeOrigin.MERGEMATCHWHERE, insert.getOptionalWhereClause(), insertTypeSvc, statementContext, evaluatorContextStmt, exprNodeErrorMessage, true));
                    }
                    int colIndex = 0;
                    for (SelectClauseElementRaw raw : insert.getSelectClause())
                    {
                        if (raw instanceof SelectClauseStreamRawSpec)
                        {
                            SelectClauseStreamRawSpec rawStreamSpec = (SelectClauseStreamRawSpec) raw;
                            Integer foundStreamNum = null;
                            for (int s = 0; s < insertTypeSvc.getStreamNames().length; s++) {
                                if (rawStreamSpec.getStreamName().equals(insertTypeSvc.getStreamNames()[s])) {
                                    foundStreamNum = s;
                                    break;
                                }
                            }
                            if (foundStreamNum == null) {
                                throw new ExprValidationException("Stream by name '" + rawStreamSpec.getStreamName() + "' was not found");
                            }
                            SelectClauseStreamCompiledSpec streamSelectSpec = new SelectClauseStreamCompiledSpec(rawStreamSpec.getStreamName(), rawStreamSpec.getOptionalAsName());
                            streamSelectSpec.setStreamNumber(foundStreamNum);
                            compiledSelect.add(streamSelectSpec);
                        }
                        else if (raw instanceof SelectClauseExprRawSpec)
                        {
                            SelectClauseExprRawSpec exprSpec = (SelectClauseExprRawSpec) raw;
                            ExprValidationContext validationContext = new ExprValidationContext(insertTypeSvc, statementContext.getMethodResolutionService(), null, statementContext.getTimeProvider(), statementContext.getVariableService(), statementContext.getTableService(), evaluatorContextStmt, statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, false);
                            ExprNode exprCompiled = ExprNodeUtility.getValidatedSubtree(ExprNodeOrigin.SELECT, exprSpec.getSelectExpression(), validationContext);
                            String resultName = exprSpec.getOptionalAsName();
                            if (resultName == null)
                            {
                                if (insert.getColumns().size() > colIndex) {
                                    resultName = insert.getColumns().get(colIndex);
                                }
                                else {
                                    resultName = ExprNodeUtility.toExpressionStringMinPrecedenceSafe(exprCompiled);
                                }
                            }
                            compiledSelect.add(new SelectClauseExprCompiledSpec(exprCompiled, resultName, exprSpec.getOptionalAsName(), exprSpec.isEvents()));
                            EPStatementStartMethodHelperValidate.validateNoAggregations(exprCompiled, "Expression in a merge-selection may not utilize aggregation functions");
                        }
                        else if (raw instanceof SelectClauseElementWildcard)
                        {
                            compiledSelect.add(new SelectClauseElementWildcard());
                        }
                        else
                        {
                            throw new IllegalStateException("Unknown select clause item:" + raw);
                        }
                        colIndex++;
                    }
                    insert.setSelectClauseCompiled(compiledSelect);
                }
                else {
                    throw new IllegalArgumentException("Unrecognized merge item '" + item.getClass().getName() + "'");
                }
            }
        }
    }

    // For delete actions from named windows
    protected ExprNode validateJoinNamedWindow(String engineURI,
                                         StatementContext statementContext,
                                         ExprNodeOrigin exprNodeOrigin,
                                         ExprNode deleteJoinExpr,
                                         EventType namedWindowType,
                                         String namedWindowStreamName,
                                         String namedWindowName,
                                         EventType filteredType,
                                         String filterStreamName,
                                         String filteredTypeName,
                                         String optionalTableName
                                         ) throws ExprValidationException
    {
        if (deleteJoinExpr == null)
        {
            return null;
        }

        LinkedHashMap<String, Pair<EventType, String>> namesAndTypes = new LinkedHashMap<String, Pair<EventType, String>>();
        namesAndTypes.put(namedWindowStreamName, new Pair<EventType, String>(namedWindowType, namedWindowName));
        namesAndTypes.put(filterStreamName, new Pair<EventType, String>(filteredType, filteredTypeName));
        StreamTypeService typeService = new StreamTypeServiceImpl(namesAndTypes, engineURI, false, false);

        ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(statementContext, false);
        ExprValidationContext validationContext = new ExprValidationContext(typeService, statementContext.getMethodResolutionService(), null, statementContext.getSchedulingService(), statementContext.getVariableService(), statementContext.getTableService(), evaluatorContextStmt, statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations(), statementContext.getContextDescriptor(), false, false, true, false, null, false);
        return ExprNodeUtility.getValidatedSubtree(exprNodeOrigin, deleteJoinExpr, validationContext);
    }

    private static class ContextFactoryResult {
        private final StatementAgentInstanceFactoryOnTriggerBase contextFactory;
        private final SubSelectStrategyCollection subSelectStrategyCollection;
        private final ResultSetProcessorFactoryDesc resultSetProcessorPrototype;

        private ContextFactoryResult(StatementAgentInstanceFactoryOnTriggerBase contextFactory, SubSelectStrategyCollection subSelectStrategyCollection, ResultSetProcessorFactoryDesc resultSetProcessorPrototype) {
            this.contextFactory = contextFactory;
            this.subSelectStrategyCollection = subSelectStrategyCollection;
            this.resultSetProcessorPrototype = resultSetProcessorPrototype;
        }

        public StatementAgentInstanceFactoryOnTriggerBase getContextFactory() {
            return contextFactory;
        }

        public SubSelectStrategyCollection getSubSelectStrategyCollection() {
            return subSelectStrategyCollection;
        }

        public ResultSetProcessorFactoryDesc getResultSetProcessorPrototype() {
            return resultSetProcessorPrototype;
        }
    }

    private static class ActivatorResult {
        private final ViewableActivator activator;
        private final String triggerEventTypeName;
        private final EventType activatorResultEventType;

        private ActivatorResult(ViewableActivator activator, String triggerEventTypeName, EventType activatorResultEventType) {
            this.activator = activator;
            this.triggerEventTypeName = triggerEventTypeName;
            this.activatorResultEventType = activatorResultEventType;
        }
    }

    private static class TriggerValidationPlanResult {
        private final SubSelectStrategyCollection subSelectStrategyCollection;
        private final ResultSetProcessorFactoryDesc resultSetProcessorPrototype;
        private final ExprNode validatedJoin;
        private final String zeroStreamAliasName;

        private TriggerValidationPlanResult(SubSelectStrategyCollection subSelectStrategyCollection, ResultSetProcessorFactoryDesc resultSetProcessorPrototype, ExprNode validatedJoin, String zeroStreamAliasName) {
            this.subSelectStrategyCollection = subSelectStrategyCollection;
            this.resultSetProcessorPrototype = resultSetProcessorPrototype;
            this.validatedJoin = validatedJoin;
            this.zeroStreamAliasName = zeroStreamAliasName;
        }

        public SubSelectStrategyCollection getSubSelectStrategyCollection() {
            return subSelectStrategyCollection;
        }

        public ResultSetProcessorFactoryDesc getResultSetProcessorPrototype() {
            return resultSetProcessorPrototype;
        }

        public ExprNode getValidatedJoin() {
            return validatedJoin;
        }

        public String getZeroStreamAliasName() {
            return zeroStreamAliasName;
        }
    }
}

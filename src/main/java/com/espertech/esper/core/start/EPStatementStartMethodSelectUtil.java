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
import com.espertech.esper.client.annotation.HookType;
import com.espertech.esper.client.annotation.IterableUnbound;
import com.espertech.esper.client.hook.SQLColumnTypeConversion;
import com.espertech.esper.client.hook.SQLOutputRowConversion;
import com.espertech.esper.core.context.activator.ViewableActivationResult;
import com.espertech.esper.core.context.activator.ViewableActivator;
import com.espertech.esper.core.context.activator.ViewableActivatorFactory;
import com.espertech.esper.core.context.activator.ViewableActivatorTable;
import com.espertech.esper.core.context.factory.StatementAgentInstanceFactorySelect;
import com.espertech.esper.core.context.subselect.SubSelectActivationCollection;
import com.espertech.esper.core.context.subselect.SubSelectStrategyCollection;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.context.util.ContextPropertyRegistry;
import com.espertech.esper.core.context.util.EPStatementAgentInstanceHandle;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.core.service.ExprEvaluatorContextStatement;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.core.service.StreamJoinAnalysisResult;
import com.espertech.esper.epl.annotation.AnnotationUtil;
import com.espertech.esper.epl.core.*;
import com.espertech.esper.epl.db.DatabasePollingViewableFactory;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.join.base.HistoricalViewableDesc;
import com.espertech.esper.epl.join.base.JoinSetComposerPrototype;
import com.espertech.esper.epl.join.base.JoinSetComposerPrototypeFactory;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.named.NamedWindowProcessorInstance;
import com.espertech.esper.epl.named.NamedWindowService;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.util.EPLValidationUtil;
import com.espertech.esper.epl.view.OutputProcessViewCallback;
import com.espertech.esper.epl.view.OutputProcessViewFactory;
import com.espertech.esper.epl.view.OutputProcessViewFactoryFactory;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.epl.virtualdw.VirtualDWViewProviderForAgentInstance;
import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.metrics.instrumentation.InstrumentationAgent;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.pattern.EvalRootFactoryNode;
import com.espertech.esper.pattern.PatternContext;
import com.espertech.esper.rowregex.EventRowRegexNFAViewFactory;
import com.espertech.esper.util.CollectionUtil;
import com.espertech.esper.util.JavaClassHelper;
import com.espertech.esper.util.StopCallback;
import com.espertech.esper.view.HistoricalEventViewable;
import com.espertech.esper.view.ViewFactoryChain;

import java.util.LinkedList;
import java.util.List;

/**
 * Starts and provides the stop method for EPL statements.
 */
public class EPStatementStartMethodSelectUtil
{
    public static EPStatementStartMethodSelectDesc prepare(StatementSpecCompiled statementSpec,
                                                           EPServicesContext services,
                                                           StatementContext statementContext,
                                                           boolean recoveringResilient,
                                                           AgentInstanceContext defaultAgentInstanceContext,
                                                           boolean queryPlanLogging,
                                                           ViewableActivatorFactory optionalViewableActivatorFactory,
                                                           OutputProcessViewCallback optionalOutputProcessViewCallback,
                                                           SelectExprProcessorDeliveryCallback selectExprProcessorDeliveryCallback)
        throws ExprValidationException {

        // define stop and destroy
        final List<StopCallback> stopCallbacks = new LinkedList<StopCallback>();
        EPStatementDestroyCallbackList destroyCallbacks = new EPStatementDestroyCallbackList();

        // determine context
        final String contextName = statementSpec.getOptionalContextName();
        final ContextPropertyRegistry contextPropertyRegistry = (contextName != null) ? services.getContextManagementService().getContextDescriptor(contextName).getContextPropertyRegistry() : null;

        // Determine stream names for each stream - some streams may not have a name given
        String[] streamNames = EPStatementStartMethodHelperUtil.determineStreamNames(statementSpec.getStreamSpecs());
        int numStreams = streamNames.length;
        if (numStreams == 0) {
            throw new ExprValidationException("The from-clause is required but has not been specified");
        }
        final boolean isJoin = statementSpec.getStreamSpecs().length > 1;
        final boolean hasContext = statementSpec.getOptionalContextName() != null;

        // First we create streams for subselects, if there are any
        SubSelectActivationCollection subSelectStreamDesc = EPStatementStartMethodHelperSubselect.createSubSelectActivation(services, statementSpec, statementContext, destroyCallbacks);

        // Create streams and views
        ViewableActivator[] eventStreamParentViewableActivators = new ViewableActivator[numStreams];
        ViewFactoryChain[] unmaterializedViewChain = new ViewFactoryChain[numStreams];
        String[] eventTypeNames = new String[numStreams];
        boolean[] isNamedWindow = new boolean[numStreams];
        HistoricalEventViewable[] historicalEventViewables = new HistoricalEventViewable[numStreams];

        // verify for joins that required views are present
        StreamJoinAnalysisResult joinAnalysisResult = verifyJoinViews(statementSpec, statementContext.getNamedWindowService(), defaultAgentInstanceContext);
        final ExprEvaluatorContextStatement evaluatorContextStmt = new ExprEvaluatorContextStatement(statementContext, false);

        for (int i = 0; i < statementSpec.getStreamSpecs().length; i++)
        {
            StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[i];

            boolean isCanIterateUnbound = streamSpec.getViewSpecs().length == 0 &&
                    (services.getConfigSnapshot().getEngineDefaults().getViewResources().isIterableUnbound() ||
                            AnnotationUtil.findAnnotation(statementSpec.getAnnotations(), IterableUnbound.class) != null);

            // Create view factories and parent view based on a filter specification
            if (streamSpec instanceof FilterStreamSpecCompiled)
            {
                final FilterStreamSpecCompiled filterStreamSpec = (FilterStreamSpecCompiled) streamSpec;
                eventTypeNames[i] = filterStreamSpec.getFilterSpec().getFilterForEventTypeName();

                // Since only for non-joins we get the existing stream's lock and try to reuse it's views
                final boolean filterSubselectSameStream = EPStatementStartMethodHelperUtil.determineSubquerySameStream(statementSpec, filterStreamSpec);

                // create activator
                ViewableActivator activatorDeactivator;
                if (optionalViewableActivatorFactory != null) {
                    activatorDeactivator = optionalViewableActivatorFactory.createActivatorSimple(filterStreamSpec);
                    if (activatorDeactivator == null) {
                        throw new IllegalStateException("Viewable activate is null for " + filterStreamSpec.getFilterSpec().getFilterForEventType().getName());
                    }
                }
                else {
                    if (!hasContext) {
                        activatorDeactivator = services.getViewableActivatorFactory().createStreamReuseView(services, statementContext, statementSpec, filterStreamSpec, isJoin, evaluatorContextStmt, filterSubselectSameStream, i, isCanIterateUnbound);
                    }
                    else {
                        InstrumentationAgent instrumentationAgentFilter = null;
                        if (InstrumentationHelper.ENABLED) {
                            final String eventTypeName = filterStreamSpec.getFilterSpec().getFilterForEventType().getName();
                            final int streamNumber = i;
                            instrumentationAgentFilter = new InstrumentationAgent() {
                                public void indicateQ() {
                                    InstrumentationHelper.get().qFilterActivationStream(eventTypeName, streamNumber);
                                }
                                public void indicateA() {
                                    InstrumentationHelper.get().aFilterActivationStream();
                                }
                            };
                        }

                        activatorDeactivator = services.getViewableActivatorFactory().createFilterProxy(services, filterStreamSpec.getFilterSpec(), statementSpec.getAnnotations(), false, instrumentationAgentFilter, isCanIterateUnbound);
                    }
                }
                eventStreamParentViewableActivators[i] = activatorDeactivator;

                EventType resultEventType = filterStreamSpec.getFilterSpec().getResultEventType();
                unmaterializedViewChain[i] = services.getViewService().createFactories(i, resultEventType, streamSpec.getViewSpecs(), streamSpec.getOptions(), statementContext);
            }
            // Create view factories and parent view based on a pattern expression
            else if (streamSpec instanceof PatternStreamSpecCompiled)
            {
                PatternStreamSpecCompiled patternStreamSpec = (PatternStreamSpecCompiled) streamSpec;
                boolean usedByChildViews = streamSpec.getViewSpecs().length > 0 || (statementSpec.getInsertIntoDesc() != null);
                String patternTypeName = statementContext.getStatementId() + "_pattern_" + i;
                final EventType eventType = services.getEventAdapterService().createSemiAnonymousMapType(patternTypeName, patternStreamSpec.getTaggedEventTypes(), patternStreamSpec.getArrayEventTypes(), usedByChildViews);
                unmaterializedViewChain[i] = services.getViewService().createFactories(i, eventType, streamSpec.getViewSpecs(), streamSpec.getOptions(), statementContext);

                final EvalRootFactoryNode rootFactoryNode = services.getPatternNodeFactory().makeRootNode(patternStreamSpec.getEvalFactoryNode());
                final PatternContext patternContext = statementContext.getPatternContextFactory().createContext(statementContext, i, rootFactoryNode, patternStreamSpec.getMatchedEventMapMeta(), true);

                // create activator
                ViewableActivator patternActivator = services.getViewableActivatorFactory().createPattern(patternContext, rootFactoryNode, eventType, EPStatementStartMethodHelperUtil.isConsumingFilters(patternStreamSpec.getEvalFactoryNode()), patternStreamSpec.isSuppressSameEventMatches(), patternStreamSpec.isDiscardPartialsOnMatch(), isCanIterateUnbound);
                eventStreamParentViewableActivators[i] = patternActivator;
            }
            // Create view factories and parent view based on a database SQL statement
            else if (streamSpec instanceof DBStatementStreamSpec)
            {
                validateNoViews(streamSpec, "Historical data");
                DBStatementStreamSpec sqlStreamSpec = (DBStatementStreamSpec) streamSpec;
                SQLColumnTypeConversion typeConversionHook = (SQLColumnTypeConversion) JavaClassHelper.getAnnotationHook(statementSpec.getAnnotations(), HookType.SQLCOL, SQLColumnTypeConversion.class, statementContext.getMethodResolutionService());
                SQLOutputRowConversion outputRowConversionHook = (SQLOutputRowConversion) JavaClassHelper.getAnnotationHook(statementSpec.getAnnotations(), HookType.SQLROW, SQLOutputRowConversion.class, statementContext.getMethodResolutionService());
                EPStatementAgentInstanceHandle epStatementAgentInstanceHandle = defaultAgentInstanceContext.getEpStatementAgentInstanceHandle();
                final HistoricalEventViewable historicalEventViewable = DatabasePollingViewableFactory.createDBStatementView(statementContext.getStatementId(), i, sqlStreamSpec, services.getDatabaseRefService(), services.getEventAdapterService(), epStatementAgentInstanceHandle, typeConversionHook, outputRowConversionHook,
                        statementContext.getConfigSnapshot().getEngineDefaults().getLogging().isEnableJDBC());
                historicalEventViewables[i] = historicalEventViewable;
                unmaterializedViewChain[i] = ViewFactoryChain.fromTypeNoViews(historicalEventViewable.getEventType());
                eventStreamParentViewableActivators[i] = new ViewableActivator() {
                    public ViewableActivationResult activate(AgentInstanceContext agentInstanceContext, boolean isSubselect, boolean isRecoveringResilient) {
                        return new ViewableActivationResult(historicalEventViewable, CollectionUtil.STOP_CALLBACK_NONE, null, null, null, false, false, null);
                    }
                };
                stopCallbacks.add(historicalEventViewable);
            }
            else if (streamSpec instanceof MethodStreamSpec)
            {
                validateNoViews(streamSpec, "Method data");
                MethodStreamSpec methodStreamSpec = (MethodStreamSpec) streamSpec;
                EPStatementAgentInstanceHandle epStatementAgentInstanceHandle = defaultAgentInstanceContext.getEpStatementAgentInstanceHandle();
                final HistoricalEventViewable historicalEventViewable = MethodPollingViewableFactory.createPollMethodView(i, methodStreamSpec, services.getEventAdapterService(), epStatementAgentInstanceHandle, statementContext.getMethodResolutionService(), services.getEngineImportService(), statementContext.getSchedulingService(), statementContext.getScheduleBucket(), evaluatorContextStmt, statementContext.getVariableService(), statementContext.getContextName());
                historicalEventViewables[i] = historicalEventViewable;
                unmaterializedViewChain[i] = ViewFactoryChain.fromTypeNoViews(historicalEventViewable.getEventType());
                eventStreamParentViewableActivators[i] = new ViewableActivator() {
                    public ViewableActivationResult activate(AgentInstanceContext agentInstanceContext, boolean isSubselect, boolean isRecoveringResilient) {
                        return new ViewableActivationResult(historicalEventViewable, CollectionUtil.STOP_CALLBACK_NONE, null, null, null, false, false, null);
                    }
                };
                stopCallbacks.add(historicalEventViewable);
            }
            else if (streamSpec instanceof TableQueryStreamSpec)
            {
                validateNoViews(streamSpec, "Table data");
                TableQueryStreamSpec tableStreamSpec = (TableQueryStreamSpec) streamSpec;
                if (isJoin && tableStreamSpec.getFilterExpressions().size() > 0) {
                    throw new ExprValidationException("Joins with tables do not allow table filter expressions, please add table filters to the where-clause instead");
                }
                TableMetadata metadata = services.getTableService().getTableMetadata(tableStreamSpec.getTableName());
                ExprEvaluator[] tableFilterEvals = null;
                if (tableStreamSpec.getFilterExpressions().size() > 0) {
                    tableFilterEvals = ExprNodeUtility.getEvaluators(tableStreamSpec.getFilterExpressions());
                }
                EPLValidationUtil.validateContextName(true, metadata.getTableName(), metadata.getContextName(), statementSpec.getOptionalContextName(), false);
                eventStreamParentViewableActivators[i] = new ViewableActivatorTable(metadata, tableFilterEvals);
                unmaterializedViewChain[i] = ViewFactoryChain.fromTypeNoViews(metadata.getInternalEventType());
                eventTypeNames[i] = tableStreamSpec.getTableName();
                joinAnalysisResult.setTablesForStream(i, metadata);
                if (tableStreamSpec.getOptions().isUnidirectional()) {
                    throw new ExprValidationException("Tables cannot be marked as unidirectional");
                }
                if (tableStreamSpec.getOptions().isRetainIntersection() || tableStreamSpec.getOptions().isRetainUnion()) {
                    throw new ExprValidationException("Tables cannot be marked with retain");
                }
                if (isJoin) {
                    destroyCallbacks.addCallback(new EPStatementDestroyCallbackTableIdxRef(services.getTableService(), metadata, statementContext.getStatementName()));
                }
            }
            else if (streamSpec instanceof NamedWindowConsumerStreamSpec)
            {
                final NamedWindowConsumerStreamSpec namedSpec = (NamedWindowConsumerStreamSpec) streamSpec;
                final NamedWindowProcessor processor = services.getNamedWindowService().getProcessor(namedSpec.getWindowName());
                EventType namedWindowType = processor.getTailView().getEventType();
                if (namedSpec.getOptPropertyEvaluator() != null) {
                    namedWindowType = namedSpec.getOptPropertyEvaluator().getFragmentEventType();
                }

                eventStreamParentViewableActivators[i] = services.getViewableActivatorFactory().createNamedWindow(processor, namedSpec.getFilterExpressions(), namedSpec.getOptPropertyEvaluator());
                unmaterializedViewChain[i] = services.getViewService().createFactories(i, namedWindowType, namedSpec.getViewSpecs(), namedSpec.getOptions(), statementContext);
                joinAnalysisResult.setNamedWindow(i);
                eventTypeNames[i] = namedSpec.getWindowName();
                isNamedWindow[i] = true;

                // Consumers to named windows cannot declare a data window view onto the named window to avoid duplicate remove streams
                EPStatementStartMethodHelperValidate.validateNoDataWindowOnNamedWindow(unmaterializedViewChain[i].getViewFactoryChain());
            }
            else
            {
                throw new ExprValidationException("Unknown stream specification type: " + streamSpec);
            }
        }

        // handle match-recognize pattern
        if (statementSpec.getMatchRecognizeSpec() != null)
        {
            if (isJoin) {
                throw new ExprValidationException("Joins are not allowed when using match-recognize");
            }
            if (joinAnalysisResult.getTablesPerStream()[0] != null) {
                throw new ExprValidationException("Tables cannot be used with match-recognize");
            }
            boolean isUnbound = (unmaterializedViewChain[0].getViewFactoryChain().isEmpty()) && (!(statementSpec.getStreamSpecs()[0] instanceof NamedWindowConsumerStreamSpec));
            EventRowRegexNFAViewFactory factory = services.getRegexHandlerFactory().makeViewFactory(unmaterializedViewChain[0], statementSpec.getMatchRecognizeSpec(), defaultAgentInstanceContext, isUnbound, statementSpec.getAnnotations(), services.getConfigSnapshot().getEngineDefaults().getMatchRecognize());
            unmaterializedViewChain[0].getViewFactoryChain().add(factory);

            EPStatementStartMethodHelperAssignExpr.assignAggregations(factory.getAggregationService(), factory.getAggregationExpressions());
        }

        // Obtain event types from view factory chains
        EventType[] streamEventTypes = new EventType[statementSpec.getStreamSpecs().length];
        for (int i = 0; i < unmaterializedViewChain.length; i++)
        {
            streamEventTypes[i] = unmaterializedViewChain[i].getEventType();
        }

        // Add uniqueness information useful for joins
        joinAnalysisResult.addUniquenessInfo(unmaterializedViewChain, statementSpec.getAnnotations());

        // Validate sub-select views
        SubSelectStrategyCollection subSelectStrategyCollection = EPStatementStartMethodHelperSubselect.planSubSelect(services, statementContext, queryPlanLogging, subSelectStreamDesc, streamNames, streamEventTypes, eventTypeNames, statementSpec.getDeclaredExpressions(), contextPropertyRegistry);

        // Construct type information per stream
        StreamTypeService typeService = new StreamTypeServiceImpl(streamEventTypes, streamNames, EPStatementStartMethodHelperUtil.getHasIStreamOnly(isNamedWindow, unmaterializedViewChain), services.getEngineURI(), false);
        ViewResourceDelegateUnverified viewResourceDelegateUnverified = new ViewResourceDelegateUnverified();

        // Validate views that require validation, specifically streams that don't have
        // sub-views such as DB SQL joins
        HistoricalViewableDesc historicalViewableDesc = new HistoricalViewableDesc(numStreams);
        for (int stream = 0; stream < historicalEventViewables.length; stream++)
        {
            HistoricalEventViewable historicalEventViewable = historicalEventViewables[stream];
            if (historicalEventViewable == null) {
                continue;
            }
            historicalEventViewable.validate(services.getEngineImportService(),
                    typeService,
                    statementContext.getMethodResolutionService(),
                    statementContext.getTimeProvider(),
                    statementContext.getVariableService(), statementContext.getTableService(), evaluatorContextStmt,
                    services.getConfigSnapshot(), services.getSchedulingService(), services.getEngineURI(),
                    statementSpec.getSqlParameters(),
                    statementContext.getEventAdapterService(), statementContext.getStatementName(), statementContext.getStatementId(), statementContext.getAnnotations());
            historicalViewableDesc.setHistorical(stream, historicalEventViewable.getRequiredStreams());
            if (historicalEventViewable.getRequiredStreams().contains(stream))
            {
                throw new ExprValidationException("Parameters for historical stream " + stream + " indicate that the stream is subordinate to itself as stream parameters originate in the same stream");
            }
        }

        // unidirectional is not supported with into-table
        if (joinAnalysisResult.isUnidirectional() && statementSpec.getIntoTableSpec() != null) {
            throw new ExprValidationException("Into-table does not allow unidirectional joins");
        }

        // Construct a processor for results posted by views and joins, which takes care of aggregation if required.
        // May return null if we don't need to post-process results posted by views or joins.
        ResultSetProcessorFactoryDesc resultSetProcessorPrototypeDesc = ResultSetProcessorFactoryFactory.getProcessorPrototype(
                statementSpec, statementContext, typeService, viewResourceDelegateUnverified, joinAnalysisResult.getUnidirectionalInd(), true, contextPropertyRegistry, selectExprProcessorDeliveryCallback, services.getConfigSnapshot());

        // Validate where-clause filter tree, outer join clause and output limit expression
        EPStatementStartMethodHelperValidate.validateNodes(statementSpec, statementContext, typeService, viewResourceDelegateUnverified);

        // Handle 'prior' function nodes in terms of view requirements
        ViewResourceDelegateVerified viewResourceDelegateVerified = EPStatementStartMethodHelperViewResources.verifyPreviousAndPriorRequirements(unmaterializedViewChain, viewResourceDelegateUnverified);

        // handle join
        JoinSetComposerPrototype joinSetComposerPrototype = null;
        if (numStreams > 1) {
            boolean selectsRemoveStream = statementSpec.getSelectStreamSelectorEnum().isSelectsRStream() ||
                    statementSpec.getOutputLimitSpec() != null;
            boolean hasAggregations = !resultSetProcessorPrototypeDesc.getAggregationServiceFactoryDesc().getExpressions().isEmpty();
            joinSetComposerPrototype = JoinSetComposerPrototypeFactory.makeComposerPrototype(
                    statementContext.getStatementName(), statementContext.getStatementId(),
                    statementSpec.getOuterJoinDescList(), statementSpec.getFilterRootNode(), typeService.getEventTypes(), streamNames,
                    joinAnalysisResult, queryPlanLogging, statementContext, historicalViewableDesc, defaultAgentInstanceContext,
                    selectsRemoveStream, hasAggregations, services.getTableService(), false);
        }

        // obtain factory for output limiting
        OutputProcessViewFactory outputViewFactory = OutputProcessViewFactoryFactory.make(statementSpec, services.getInternalEventRouter(), statementContext, resultSetProcessorPrototypeDesc.getResultSetProcessorFactory().getResultEventType(), optionalOutputProcessViewCallback, services.getTableService(), resultSetProcessorPrototypeDesc.getResultSetProcessorFactory().getResultSetProcessorType());

        // Factory for statement-context instances
        StatementAgentInstanceFactorySelect factory = new StatementAgentInstanceFactorySelect(
                numStreams, eventStreamParentViewableActivators,
                statementContext, statementSpec, services,
                typeService, unmaterializedViewChain, resultSetProcessorPrototypeDesc, joinAnalysisResult, recoveringResilient,
                joinSetComposerPrototype, subSelectStrategyCollection, viewResourceDelegateVerified, outputViewFactory);

        final EPStatementStopMethod stopMethod = new EPStatementStopMethodImpl(statementContext, stopCallbacks);
        return new EPStatementStartMethodSelectDesc(factory, subSelectStrategyCollection, viewResourceDelegateUnverified, resultSetProcessorPrototypeDesc, stopMethod, destroyCallbacks);
    }

    private static void validateNoViews(StreamSpecCompiled streamSpec, String conceptName)
            throws ExprValidationException
    {
        if (streamSpec.getViewSpecs().length > 0) {
            throw new ExprValidationException(conceptName + " joins do not allow views onto the data, view '"
                    + streamSpec.getViewSpecs()[0].getObjectNamespace() + ':' + streamSpec.getViewSpecs()[0].getObjectName() + "' is not valid in this context");
        }
    }

    private static StreamJoinAnalysisResult verifyJoinViews(StatementSpecCompiled statementSpec, NamedWindowService namedWindowService, AgentInstanceContext defaultAgentInstanceContext)
            throws ExprValidationException
    {
        StreamSpecCompiled[] streamSpecs = statementSpec.getStreamSpecs();
        StreamJoinAnalysisResult analysisResult = new StreamJoinAnalysisResult(streamSpecs.length);
        if (streamSpecs.length < 2)
        {
            return analysisResult;
        }

        // Determine if any stream has a unidirectional keyword

        // inspect unidirectional indicator and named window flags
        int unidirectionalStreamNumber = -1;
        for (int i = 0; i < statementSpec.getStreamSpecs().length; i++)
        {
            StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[i];
            if (streamSpec.getOptions().isUnidirectional())
            {
                analysisResult.setUnidirectionalInd(i);
                if (unidirectionalStreamNumber != -1)
                {
                    throw new ExprValidationException("The unidirectional keyword can only apply to one stream in a join");
                }
                unidirectionalStreamNumber = i;
            }
            if (streamSpec.getViewSpecs().length > 0)
            {
                analysisResult.setHasChildViews(i);
            }
            if (streamSpec instanceof NamedWindowConsumerStreamSpec)
            {
                NamedWindowConsumerStreamSpec nwSpec = (NamedWindowConsumerStreamSpec) streamSpec;
                if (nwSpec.getOptPropertyEvaluator() != null && !streamSpec.getOptions().isUnidirectional()) {
                    throw new ExprValidationException("Failed to validate named window use in join, contained-event is only allowed for named windows when marked as unidirectional");
                }
                analysisResult.setNamedWindow(i);
                final NamedWindowProcessor processor = namedWindowService.getProcessor(nwSpec.getWindowName());
                NamedWindowProcessorInstance processorInstance = processor.getProcessorInstance(defaultAgentInstanceContext);
                String[][] uniqueIndexes = processor.getUniqueIndexes(processorInstance);
                analysisResult.getUniqueKeys()[i] = uniqueIndexes;
                if (processor.isVirtualDataWindow()) {
                    analysisResult.getViewExternal()[i] = new VirtualDWViewProviderForAgentInstance() {
                        public VirtualDWView getView(AgentInstanceContext agentInstanceContext) {
                            return processor.getProcessorInstance(agentInstanceContext).getRootViewInstance().getVirtualDataWindow();
                        }
                    };
                }
            }
        }
        if ((unidirectionalStreamNumber != -1) && (analysisResult.getHasChildViews()[unidirectionalStreamNumber]))
        {
            throw new ExprValidationException("The unidirectional keyword requires that no views are declared onto the stream");
        }
        analysisResult.setUnidirectionalStreamNumber(unidirectionalStreamNumber);

        // count streams that provide data, excluding streams that poll data (DB and method)
        int countProviderNonpolling = 0;
        for (int i = 0; i < statementSpec.getStreamSpecs().length; i++)
        {
            StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[i];
            if ((streamSpec instanceof MethodStreamSpec) ||
                (streamSpec instanceof DBStatementStreamSpec) ||
                (streamSpec instanceof TableQueryStreamSpec))
            {
                continue;
            }
            countProviderNonpolling++;
        }

        // if there is only one stream providing data, the analysis is done
        if (countProviderNonpolling == 1)
        {
            return analysisResult;
        }
        // there are multiple driving streams, verify the presence of a view for insert/remove stream

        // validation of join views works differently for unidirectional as there can be self-joins that don't require a view
        // see if this is a self-join in which all streams are filters and filter specification is the same.
        FilterSpecCompiled unidirectionalFilterSpec = null;
        FilterSpecCompiled lastFilterSpec = null;
        boolean pureSelfJoin = true;
        for (StreamSpecCompiled streamSpec : statementSpec.getStreamSpecs())
        {
            if (!(streamSpec instanceof FilterStreamSpecCompiled))
            {
                pureSelfJoin = false;
                continue;
            }

            FilterSpecCompiled filterSpec = ((FilterStreamSpecCompiled) streamSpec).getFilterSpec();
            if ((lastFilterSpec != null) && (!lastFilterSpec.equalsTypeAndFilter(filterSpec)))
            {
                pureSelfJoin = false;
            }
            if (streamSpec.getViewSpecs().length > 0)
            {
                pureSelfJoin = false;
            }
            lastFilterSpec = filterSpec;

            if (streamSpec.getOptions().isUnidirectional())
            {
                unidirectionalFilterSpec = filterSpec;
            }
        }

        // self-join without views and not unidirectional
        if ((pureSelfJoin) && (unidirectionalFilterSpec == null))
        {
            analysisResult.setPureSelfJoin(true);
            return analysisResult;
        }

        // weed out filter and pattern streams that don't have a view in a join
        for (int i = 0; i < statementSpec.getStreamSpecs().length; i++)
        {
            StreamSpecCompiled streamSpec = statementSpec.getStreamSpecs()[i];
            if (streamSpec.getViewSpecs().length > 0)
            {
                continue;
            }

            String name = streamSpec.getOptionalStreamName();
            if ((name == null) && (streamSpec instanceof FilterStreamSpecCompiled))
            {
                name = ((FilterStreamSpecCompiled) streamSpec).getFilterSpec().getFilterForEventTypeName();
            }
            if ((name == null) && (streamSpec instanceof PatternStreamSpecCompiled))
            {
                name = "pattern event stream";
            }

            if (streamSpec.getOptions().isUnidirectional())
            {
                continue;
            }
            // allow a self-join without a child view, in that the filter spec is the same as the unidirection's stream filter
            if ((unidirectionalFilterSpec != null) &&
                (streamSpec instanceof FilterStreamSpecCompiled) &&
                (((FilterStreamSpecCompiled) streamSpec).getFilterSpec().equalsTypeAndFilter(unidirectionalFilterSpec)))
            {
                analysisResult.setUnidirectionalNonDriving(i);
                continue;
            }
            if ((streamSpec instanceof FilterStreamSpecCompiled) ||
                (streamSpec instanceof PatternStreamSpecCompiled))
            {
                throw new ExprValidationException("Joins require that at least one view is specified for each stream, no view was specified for " + name);
            }
        }

        return analysisResult;
    }
}

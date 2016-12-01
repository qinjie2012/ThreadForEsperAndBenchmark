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
import com.espertech.esper.client.annotation.HintEnum;
import com.espertech.esper.core.context.activator.ViewableActivator;
import com.espertech.esper.core.context.factory.StatementAgentInstanceFactoryCreateWindow;
import com.espertech.esper.core.context.factory.StatementAgentInstanceFactoryCreateWindowResult;
import com.espertech.esper.core.context.mgr.ContextManagedStatementCreateWindowDesc;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.context.util.ContextMergeView;
import com.espertech.esper.core.context.util.EPStatementAgentInstanceHandle;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.core.service.resource.StatementResourceHolder;
import com.espertech.esper.epl.core.ResultSetProcessorFactoryDesc;
import com.espertech.esper.epl.core.ResultSetProcessorFactoryFactory;
import com.espertech.esper.epl.core.StreamTypeService;
import com.espertech.esper.epl.core.StreamTypeServiceImpl;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.named.NamedWindowService;
import com.espertech.esper.epl.spec.FilterStreamSpecCompiled;
import com.espertech.esper.epl.spec.SelectClauseElementWildcard;
import com.espertech.esper.epl.spec.SelectClauseStreamSelectorEnum;
import com.espertech.esper.epl.spec.StatementSpecCompiled;
import com.espertech.esper.epl.view.OutputProcessViewFactory;
import com.espertech.esper.epl.view.OutputProcessViewFactoryFactory;
import com.espertech.esper.epl.virtualdw.VirtualDWViewFactory;
import com.espertech.esper.event.vaevent.ValueAddEventProcessor;
import com.espertech.esper.metrics.instrumentation.InstrumentationAgent;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.util.StopCallback;
import com.espertech.esper.view.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Starts and provides the stop method for EPL statements.
 */
public class EPStatementStartMethodCreateWindow extends EPStatementStartMethodBase
{
    public EPStatementStartMethodCreateWindow(StatementSpecCompiled statementSpec) {
        super(statementSpec);
    }

    public EPStatementStartResult startInternal(final EPServicesContext services, final StatementContext statementContext, boolean isNewStatement, boolean isRecoveringStatement, boolean isRecoveringResilient) throws ExprValidationException, ViewProcessingException {

        // define stop
        final List<StopCallback> stopCallbacks = new ArrayList<StopCallback>();

        // determine context
        final String contextName = statementSpec.getOptionalContextName();

        // register agent instance resources for use in HA
        EPStatementAgentInstanceHandle epStatementAgentInstanceHandle = getDefaultAgentInstanceHandle(statementContext);
        if (services.getSchedulableAgentInstanceDirectory() != null) {
            services.getSchedulableAgentInstanceDirectory().add(epStatementAgentInstanceHandle);
        }

        // Create view factories and parent view based on a filter specification
        // Since only for non-joins we get the existing stream's lock and try to reuse it's views
        final FilterStreamSpecCompiled filterStreamSpec = (FilterStreamSpecCompiled) statementSpec.getStreamSpecs()[0];
        InstrumentationAgent instrumentationAgentCreateWindowInsert = null;
        if (InstrumentationHelper.ENABLED) {
            final String eventTypeName = filterStreamSpec.getFilterSpec().getFilterForEventType().getName();
            instrumentationAgentCreateWindowInsert = new InstrumentationAgent() {
                public void indicateQ() {
                    InstrumentationHelper.get().qFilterActivationNamedWindowInsert(eventTypeName);
                }
                public void indicateA() {
                    InstrumentationHelper.get().aFilterActivationNamedWindowInsert();
                }
            };
        }
        ViewableActivator activator = services.getViewableActivatorFactory().createFilterProxy(services, filterStreamSpec.getFilterSpec(), statementContext.getAnnotations(), false, instrumentationAgentCreateWindowInsert, false);

        // create data window view factories
        ViewFactoryChain unmaterializedViewChain = services.getViewService().createFactories(0, filterStreamSpec.getFilterSpec().getResultEventType(), filterStreamSpec.getViewSpecs(), filterStreamSpec.getOptions(), statementContext);

        // verify data window
        verifyDataWindowViewFactoryChain(unmaterializedViewChain.getViewFactoryChain());

        // get processor for variant-streams and versioned typed
        final String windowName = statementSpec.getCreateWindowDesc().getWindowName();
        ValueAddEventProcessor optionalRevisionProcessor = statementContext.getValueAddEventService().getValueAddProcessor(windowName);

        // add named window processor (one per named window for all agent instances)
        boolean isPrioritized = services.getEngineSettingsService().getEngineSettings().getExecution().isPrioritized();
        boolean isEnableSubqueryIndexShare = HintEnum.ENABLE_WINDOW_SUBQUERY_INDEXSHARE.getHint(statementSpec.getAnnotations()) != null;
        if (!isEnableSubqueryIndexShare && unmaterializedViewChain.getViewFactoryChain().get(0) instanceof VirtualDWViewFactory) {
            isEnableSubqueryIndexShare = true;  // index share is always enabled for virtual data window (otherwise it wouldn't make sense)
        }
        boolean isBatchingDataWindow = determineBatchingDataWindow(unmaterializedViewChain.getViewFactoryChain());
        final VirtualDWViewFactory virtualDataWindowFactory = determineVirtualDataWindow(unmaterializedViewChain.getViewFactoryChain());
        Set<String> optionalUniqueKeyProps = ViewServiceHelper.getUniqueCandidateProperties(unmaterializedViewChain.getViewFactoryChain(), statementSpec.getAnnotations());
        NamedWindowProcessor processor = services.getNamedWindowService().addProcessor(windowName, contextName, filterStreamSpec.getFilterSpec().getResultEventType(), statementContext.getStatementResultService(), optionalRevisionProcessor, statementContext.getExpression(), statementContext.getStatementName(), isPrioritized, isEnableSubqueryIndexShare, isBatchingDataWindow, virtualDataWindowFactory != null, statementContext.getEpStatementHandle().getMetricsHandle(), optionalUniqueKeyProps,
                statementSpec.getCreateWindowDesc().getAsEventTypeName(),
                statementContext.getStatementExtensionServicesContext().getStmtResources());

        Viewable finalViewable;
        EPStatementStopMethod stopStatementMethod;
        EPStatementDestroyMethod destroyStatementMethod;

        try {
            // add stop callback
            stopCallbacks.add(new StopCallback() {
                public void stop() {
                    services.getNamedWindowService().removeProcessor(windowName);
                    if (virtualDataWindowFactory != null) {
                        virtualDataWindowFactory.destroyNamedWindow();
                    }
                }
            });

            // Add a wildcard to the select clause as subscribers received the window contents
            statementSpec.getSelectClauseSpec().setSelectExprList(new SelectClauseElementWildcard());
            statementSpec.setSelectStreamDirEnum(SelectClauseStreamSelectorEnum.RSTREAM_ISTREAM_BOTH);

            // obtain result set processor factory
            StreamTypeService typeService = new StreamTypeServiceImpl(new EventType[] {processor.getNamedWindowType()}, new String[] {windowName}, new boolean[] {true}, services.getEngineURI(), false);
            ResultSetProcessorFactoryDesc resultSetProcessorPrototype = ResultSetProcessorFactoryFactory.getProcessorPrototype(
                    statementSpec, statementContext, typeService, null, new boolean[0], true, null, null, services.getConfigSnapshot());

            // obtain factory for output limiting
            OutputProcessViewFactory outputViewFactory = OutputProcessViewFactoryFactory.make(statementSpec, services.getInternalEventRouter(), statementContext, resultSetProcessorPrototype.getResultSetProcessorFactory().getResultEventType(), null, services.getTableService(), resultSetProcessorPrototype.getResultSetProcessorFactory().getResultSetProcessorType());

            // create context factory
            // Factory for statement-context instances
            StatementAgentInstanceFactoryCreateWindow contextFactory = new StatementAgentInstanceFactoryCreateWindow(statementContext, statementSpec, services, activator, unmaterializedViewChain, resultSetProcessorPrototype, outputViewFactory, isRecoveringStatement);

            // With context - delegate instantiation to context
            final EPStatementStopMethod stopMethod = new EPStatementStopMethodImpl(statementContext, stopCallbacks);
            if (statementSpec.getOptionalContextName() != null) {

                ContextMergeView mergeView = new ContextMergeView(processor.getNamedWindowType());
                finalViewable = mergeView;

                ContextManagedStatementCreateWindowDesc statement = new ContextManagedStatementCreateWindowDesc(statementSpec, statementContext, mergeView, contextFactory);
                services.getContextManagementService().addStatement(contextName, statement, isRecoveringResilient);
                stopStatementMethod = new EPStatementStopMethod(){
                    public void stop()
                    {
                        services.getContextManagementService().stoppedStatement(contextName, statementContext.getStatementName(), statementContext.getStatementId());
                        stopMethod.stop();
                    }
                };

                destroyStatementMethod = new EPStatementDestroyMethod(){
                    public void destroy() {
                        services.getContextManagementService().destroyedStatement(contextName, statementContext.getStatementName(), statementContext.getStatementId());
                    }
                };
            }
            // Without context - start here
            else {
                AgentInstanceContext agentInstanceContext = getDefaultAgentInstanceContext(statementContext);
                final StatementAgentInstanceFactoryCreateWindowResult resultOfStart;
                try {
                    resultOfStart = (StatementAgentInstanceFactoryCreateWindowResult) contextFactory.newContext(agentInstanceContext, isRecoveringResilient);
                }
                catch (RuntimeException ex) {
                    services.getNamedWindowService().removeProcessor(windowName);
                    throw ex;
                }
                finalViewable = resultOfStart.getFinalView();
                stopStatementMethod = new EPStatementStopMethod() {
                    public void stop() {
                        resultOfStart.getStopCallback().stop();
                        stopMethod.stop();
                    }
                };
                destroyStatementMethod = null;

                if (statementContext.getStatementExtensionServicesContext() != null && statementContext.getStatementExtensionServicesContext().getStmtResources() != null) {
                    StatementResourceHolder holder = statementContext.getStatementExtensionServicesContext().extractStatementResourceHolder(resultOfStart);
                    statementContext.getStatementExtensionServicesContext().getStmtResources().setUnpartitioned(holder);
                    statementContext.getStatementExtensionServicesContext().postProcessStart(resultOfStart, isRecoveringResilient);
                }
            }
        }
        catch (ExprValidationException ex) {
            services.getNamedWindowService().removeProcessor(windowName);
            throw ex;
        }
        catch (RuntimeException ex) {
            services.getNamedWindowService().removeProcessor(windowName);
            throw ex;
        }

        return new EPStatementStartResult(finalViewable, stopStatementMethod, destroyStatementMethod);
    }

    private static VirtualDWViewFactory determineVirtualDataWindow(List<ViewFactory> viewFactoryChain) {
        for (ViewFactory viewFactory : viewFactoryChain) {
            if (viewFactory instanceof VirtualDWViewFactory) {
                return (VirtualDWViewFactory) viewFactory;
            }
        }
        return null;
    }

    private static boolean determineBatchingDataWindow(List<ViewFactory> viewFactoryChain) {
        for (ViewFactory viewFactory : viewFactoryChain) {
            if (viewFactory instanceof DataWindowBatchingViewFactory) {
                return true;
            }
        }
        return false;
    }

    private void verifyDataWindowViewFactoryChain(List<ViewFactory> viewFactories) throws ExprValidationException {

        for (ViewFactory viewFactory : viewFactories)
        {
            if (viewFactory instanceof DataWindowViewFactory)
            {
                return;
            }
        }
        throw new ExprValidationException(NamedWindowService.ERROR_MSG_DATAWINDOWS);
    }
}

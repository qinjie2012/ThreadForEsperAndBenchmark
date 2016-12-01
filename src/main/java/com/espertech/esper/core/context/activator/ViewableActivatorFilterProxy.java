/*
 * *************************************************************************************
 *  Copyright (C) 2006-2015 EsperTech, Inc. All rights reserved.                       *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 * *************************************************************************************
 */

package com.espertech.esper.core.context.activator;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.core.service.EPStatementHandleCallback;
import com.espertech.esper.filter.*;
import com.espertech.esper.metrics.instrumentation.InstrumentationAgent;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.view.EventStream;
import com.espertech.esper.view.ZeroDepthStreamIterable;
import com.espertech.esper.view.ZeroDepthStreamNoIterate;
import com.espertech.esper.view.stream.EventStreamProxy;

import java.lang.annotation.Annotation;
import java.util.Collection;

public class ViewableActivatorFilterProxy implements ViewableActivator {

    private final EPServicesContext services;
    private final FilterSpecCompiled filterSpec;
    private final Annotation annotations[];
    private final boolean isSubSelect;
    private final InstrumentationAgent instrumentationAgent;
    private final boolean isCanIterate;

    protected ViewableActivatorFilterProxy(EPServicesContext services, FilterSpecCompiled filterSpec, Annotation[] annotations, boolean subSelect, InstrumentationAgent instrumentationAgent, boolean isCanIterate) {
        this.services = services;
        this.filterSpec = filterSpec;
        this.annotations = annotations;
        isSubSelect = subSelect;
        this.instrumentationAgent = instrumentationAgent;
        this.isCanIterate = isCanIterate;
    }

    public ViewableActivationResult activate(final AgentInstanceContext agentInstanceContext, boolean isSubselect, boolean isRecoveringResilient) {

        // New event stream
        EventType resultEventType = filterSpec.getResultEventType();
        EventStream zeroDepthStream = isCanIterate ? new ZeroDepthStreamIterable(resultEventType) : new ZeroDepthStreamNoIterate(resultEventType);

        // audit proxy
        EventStream inputStream = EventStreamProxy.getAuditProxy(agentInstanceContext.getStatementContext().getEngineURI(), agentInstanceContext.getEpStatementAgentInstanceHandle().getStatementHandle().getStatementName(), annotations, filterSpec, zeroDepthStream);

        final EventStream eventStream = inputStream;
        final String statementId = agentInstanceContext.getStatementContext().getStatementId();

        FilterHandleCallback filterCallback;
        if (filterSpec.getOptionalPropertyEvaluator() != null)
        {
            filterCallback = new FilterHandleCallback()
            {
                public String getStatementId()
                {
                    return statementId;
                }

                public void matchFound(EventBean theEvent, Collection<FilterHandleCallback> allStmtMatches)
                {
                    EventBean[] result = filterSpec.getOptionalPropertyEvaluator().getProperty(theEvent, agentInstanceContext);
                    if (result == null)
                    {
                        return;
                    }
                    eventStream.insert(result);
                }

                public boolean isSubSelect()
                {
                    return isSubSelect;
                }
            };
        }
        else
        {
            filterCallback = new FilterHandleCallback()
            {
                public String getStatementId()
                {
                    return statementId;
                }

                public void matchFound(EventBean theEvent, Collection<FilterHandleCallback> allStmtMatches)
                {
                    if (InstrumentationHelper.ENABLED) { instrumentationAgent.indicateQ();}
                    eventStream.insert(theEvent);
                    if (InstrumentationHelper.ENABLED) { instrumentationAgent.indicateA();}
                }
                public boolean isSubSelect()
                {
                    return isSubSelect;
                }
            };
        }
        EPStatementHandleCallback filterHandle = new EPStatementHandleCallback(agentInstanceContext.getEpStatementAgentInstanceHandle(), filterCallback);

        FilterValueSetParam[][] addendum = null;
        if (agentInstanceContext.getAgentInstanceFilterProxy() != null) {
            addendum = agentInstanceContext.getAgentInstanceFilterProxy().getAddendumFilters(filterSpec);
        }
        FilterValueSet filterValueSet = filterSpec.getValueSet(null, agentInstanceContext, addendum);
        FilterServiceEntry filterServiceEntry = services.getFilterService().add(filterValueSet, filterHandle);

        ViewableActivatorFilterProxyStopCallback stopCallback = new ViewableActivatorFilterProxyStopCallback(this, filterHandle, filterServiceEntry);
        return new ViewableActivationResult(inputStream, stopCallback, null, null, null, false, false, null);
    }

    public EPServicesContext getServices() {
        return services;
    }

    public FilterSpecCompiled getFilterSpec() {
        return filterSpec;
    }
}

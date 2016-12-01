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

package com.espertech.esper.core.context.mgr;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.context.util.EPStatementAgentInstanceHandle;
import com.espertech.esper.core.service.EPStatementHandleCallback;
import com.espertech.esper.core.service.EngineLevelExtensionServicesContext;
import com.espertech.esper.core.service.StatementAgentInstanceFilterVersion;
import com.espertech.esper.epl.spec.ContextDetailConditionTimePeriod;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;
import com.espertech.esper.pattern.MatchedEventMap;
import com.espertech.esper.schedule.ScheduleHandleCallback;
import com.espertech.esper.schedule.ScheduleSlot;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;

public class ContextControllerConditionTimePeriod implements ContextControllerCondition {

    private static final Log log = LogFactory.getLog(ContextControllerConditionTimePeriod.class);

    private final String contextName;
    private final AgentInstanceContext agentInstanceContext;
    private final ScheduleSlot scheduleSlot;
    private final ContextDetailConditionTimePeriod spec;
    private final ContextControllerConditionCallback callback;
    private final ContextInternalFilterAddendum filterAddendum;

    private EPStatementHandleCallback scheduleHandle;

    public ContextControllerConditionTimePeriod(String contextName, AgentInstanceContext agentInstanceContext, ScheduleSlot scheduleSlot, ContextDetailConditionTimePeriod spec, ContextControllerConditionCallback callback, ContextInternalFilterAddendum filterAddendum) {
        this.contextName = contextName;
        this.agentInstanceContext = agentInstanceContext;
        this.scheduleSlot = scheduleSlot;
        this.spec = spec;
        this.callback = callback;
        this.filterAddendum = filterAddendum;
    }

    public void activate(EventBean optionalTriggerEvent, MatchedEventMap priorMatches, long timeOffset, boolean isRecoveringResilient) {
        startContextCallback(timeOffset);
    }

    public void deactivate() {
        endContextCallback();
    }

    public boolean isRunning() {
        return scheduleHandle != null;
    }

    public boolean isImmediate() {
        return spec.isImmediate();
    }

    private void startContextCallback(long timeOffset) {
        ScheduleHandleCallback scheduleCallback = new ScheduleHandleCallback() {
            public void scheduledTrigger(EngineLevelExtensionServicesContext extensionServicesContext)
            {
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qContextScheduledEval(ContextControllerConditionTimePeriod.this.agentInstanceContext.getStatementContext().getContextDescriptor());}
                scheduleHandle = null;  // terminates automatically unless scheduled again
                callback.rangeNotification(Collections.<String, Object>emptyMap(), ContextControllerConditionTimePeriod.this, null, null, filterAddendum);
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aContextScheduledEval();}
            }
        };
        EPStatementAgentInstanceHandle agentHandle = new EPStatementAgentInstanceHandle(agentInstanceContext.getStatementContext().getEpStatementHandle(), agentInstanceContext.getStatementContext().getDefaultAgentInstanceLock(), -1, new StatementAgentInstanceFilterVersion());
        scheduleHandle = new EPStatementHandleCallback(agentHandle, scheduleCallback);

        long msec = spec.getTimePeriod().nonconstEvaluator().deltaMillisecondsUseEngineTime(null, agentInstanceContext) - timeOffset;
        agentInstanceContext.getStatementContext().getSchedulingService().add(msec, scheduleHandle, scheduleSlot);
    }

    private void endContextCallback() {
        if (scheduleHandle!= null) {
            agentInstanceContext.getStatementContext().getSchedulingService().remove(scheduleHandle, scheduleSlot);
        }
        scheduleHandle = null;
    }

    public Long getExpectedEndTime() {
        long current = agentInstanceContext.getStatementContext().getTimeProvider().getTime();
        long msec = spec.getTimePeriod().nonconstEvaluator().deltaMillisecondsAdd(current, null, true, agentInstanceContext);
        return current + msec;
    }
}

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

package com.espertech.esper.core.context.util;

import com.espertech.esper.core.context.mgr.AgentInstanceFilterProxy;
import com.espertech.esper.core.service.*;
import com.espertech.esper.epl.table.mgmt.TableExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.script.AgentInstanceScriptContext;
import com.espertech.esper.event.MappedEventBean;
import com.espertech.esper.schedule.TimeProvider;
import com.espertech.esper.util.StopCallback;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class AgentInstanceContext implements ExprEvaluatorContext {

    private final StatementContext statementContext;
    private final EPStatementAgentInstanceHandle epStatementAgentInstanceHandle;
    private final int agentInstanceId;
    private final AgentInstanceFilterProxy agentInstanceFilterProxy;
    private final MappedEventBean agentInstanceProperties;
    private final AgentInstanceScriptContext agentInstanceScriptContext;
    private final ExpressionResultCacheService expressionResultCacheService;
    private Object terminationCallbacks;

    public AgentInstanceContext(StatementContext statementContext, EPStatementAgentInstanceHandle epStatementAgentInstanceHandle, int agentInstanceId, AgentInstanceFilterProxy agentInstanceFilterProxy, MappedEventBean agentInstanceProperties, AgentInstanceScriptContext agentInstanceScriptContext) {
        this.statementContext = statementContext;
        this.epStatementAgentInstanceHandle = epStatementAgentInstanceHandle;
        this.agentInstanceId = agentInstanceId;
        this.agentInstanceFilterProxy = agentInstanceFilterProxy;
        this.agentInstanceProperties = agentInstanceProperties;
        this.agentInstanceScriptContext = agentInstanceScriptContext;
        if (statementContext.isStatelessSelect()) {
            this.expressionResultCacheService = statementContext.getExpressionResultCacheServiceSharable();
        }
        else {
            this.expressionResultCacheService = new ExpressionResultCacheServiceAgentInstance();
        }
        this.terminationCallbacks = null;
    }

    public AgentInstanceFilterProxy getAgentInstanceFilterProxy() {
        return agentInstanceFilterProxy;
    }

    public AgentInstanceScriptContext getAgentInstanceScriptContext() {
        return agentInstanceScriptContext;
    }

    public TimeProvider getTimeProvider() {
        return statementContext.getTimeProvider();
    }

    public ExpressionResultCacheService getExpressionResultCacheService() {
        return expressionResultCacheService;
    }

    public int getAgentInstanceId() {
        return agentInstanceId;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public EPStatementAgentInstanceHandle getEpStatementAgentInstanceHandle() {
        return epStatementAgentInstanceHandle;
    }

    public MappedEventBean getContextProperties() {
        return agentInstanceProperties;
    }

    public TableExprEvaluatorContext getTableExprEvaluatorContext() {
        return statementContext.getTableExprEvaluatorContext();
    }

    public Collection<StopCallback> getTerminationCallbackRO() {
        if (terminationCallbacks == null) {
            return Collections.emptyList();
        }
        else if (terminationCallbacks instanceof Collection) {
            return (Collection<StopCallback>) terminationCallbacks;
        }
        return Collections.singletonList((StopCallback) terminationCallbacks);
    }

    public void addTerminationCallback(StopCallback callback) {
        if (terminationCallbacks == null) {
            terminationCallbacks = callback;
        }
        else if (terminationCallbacks instanceof Collection) {
            ((Collection<StopCallback>) terminationCallbacks).add(callback);
        }
        else {
            StopCallback cb = (StopCallback) terminationCallbacks;
            HashSet<StopCallback> q = new HashSet<StopCallback>(2);
            q.add(cb);
            q.add(callback);
            terminationCallbacks = q;
        }
    }

    public void removeTerminationCallback(StopCallback callback) {
        if (terminationCallbacks == null) {
            return;
        }
        else if (terminationCallbacks instanceof Collection) {
            ((Collection<StopCallback>) terminationCallbacks).remove(callback);
        }
        else if (terminationCallbacks == callback) {
            terminationCallbacks = null;
        }
    }

    public String getStatementName() {
        return statementContext.getStatementName();
    }

    public String getEngineURI() {
        return statementContext.getEngineURI();
    }

    public String getStatementId() {
        return statementContext.getStatementId();
    }

    public StatementType getStatementType() {
        return statementContext.getStatementType();
    }

    public StatementAgentInstanceLock getAgentInstanceLock() {
        return epStatementAgentInstanceHandle.getStatementAgentInstanceLock();
    }

    public Object getStatementUserObject() {
        return statementContext.getStatementUserObject();
    }
}

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

package com.espertech.esper.core.context.factory;

import com.espertech.esper.client.EPException;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.epl.table.mgmt.TableStateInstance;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.named.NamedWindowProcessorInstance;
import com.espertech.esper.epl.spec.CreateIndexDesc;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.util.StopCallback;
import com.espertech.esper.view.Viewable;

public class StatementAgentInstanceFactoryCreateIndex implements StatementAgentInstanceFactory {

    private final EPServicesContext services;
    private final CreateIndexDesc spec;
    private final Viewable finalView;
    private final NamedWindowProcessor namedWindowProcessor;
    private final String tableName;

    public StatementAgentInstanceFactoryCreateIndex(EPServicesContext services, CreateIndexDesc spec, Viewable finalView, NamedWindowProcessor namedWindowProcessor, String tableName) {
        this.services = services;
        this.spec = spec;
        this.finalView = finalView;
        this.namedWindowProcessor = namedWindowProcessor;
        this.tableName = tableName;
    }

    public StatementAgentInstanceFactoryCreateIndexResult newContext(AgentInstanceContext agentInstanceContext, boolean isRecoveringResilient)
    {
        StopCallback stopCallback;
        if (namedWindowProcessor != null) {
            // handle named window index
            final NamedWindowProcessorInstance processorInstance = namedWindowProcessor.getProcessorInstance(agentInstanceContext);

            if (namedWindowProcessor.isVirtualDataWindow()) {
                final VirtualDWView virtualDWView = processorInstance.getRootViewInstance().getVirtualDataWindow();
                virtualDWView.handleStartIndex(spec);
                stopCallback = new StopCallback() {
                    public void stop() {
                        virtualDWView.handleStopIndex(spec);
                    }
                };
            }
            else {
                try {
                    processorInstance.getRootViewInstance().addExplicitIndex(spec.isUnique(), spec.getIndexName(), spec.getColumns());
                }
                catch (ExprValidationException e) {
                    throw new EPException("Failed to create index: " + e.getMessage(), e);
                }
                stopCallback = new StopCallback() {
                    public void stop() {
                    }
                };
            }
        }
        else {
            // handle table access
            try {
                TableStateInstance instance = services.getTableService().getState(tableName, agentInstanceContext.getAgentInstanceId());
                instance.addExplicitIndex(spec);
            }
            catch (ExprValidationException ex) {
                throw new EPException("Failed to create index: " + ex.getMessage(), ex);
            }
            stopCallback = new StopCallback() {
                public void stop() {
                }
            };
        }

        return new StatementAgentInstanceFactoryCreateIndexResult(finalView, stopCallback, agentInstanceContext);
    }

    public void assignExpressions(StatementAgentInstanceFactoryResult result) {
    }

    public void unassignExpressions() {
    }
}

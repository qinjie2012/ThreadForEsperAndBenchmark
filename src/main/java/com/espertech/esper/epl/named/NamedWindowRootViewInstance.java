/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.named;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.context.factory.StatementAgentInstancePostLoadIndexVisitor;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.fafquery.FireAndForgetQueryExec;
import com.espertech.esper.epl.join.table.EventTable;
import com.espertech.esper.epl.lookup.EventTableIndexRepository;
import com.espertech.esper.epl.lookup.IndexMultiKey;
import com.espertech.esper.epl.lookup.SubordWMatchExprLookupStrategy;
import com.espertech.esper.epl.spec.CreateIndexItem;
import com.espertech.esper.epl.virtualdw.VirtualDWView;
import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.view.ViewSupport;
import com.espertech.esper.view.Viewable;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * The root window in a named window plays multiple roles: It holds the indexes for deleting rows, if any on-delete statement
 * requires such indexes. Such indexes are updated when events arrive, or remove from when a data window
 * or on-delete statement expires events. The view keeps track of on-delete statements their indexes used.
 */
public class NamedWindowRootViewInstance extends ViewSupport
{
    private final NamedWindowRootView rootView;
    private final AgentInstanceContext agentInstanceContext;

    private final EventTableIndexRepository indexRepository;
    private final Map<SubordWMatchExprLookupStrategy, EventTable[]> tablePerMultiLookup;

    private Iterable<EventBean> dataWindowContents;

    public NamedWindowRootViewInstance(NamedWindowRootView rootView, AgentInstanceContext agentInstanceContext) {
        this.rootView = rootView;
        this.agentInstanceContext = agentInstanceContext;

        this.indexRepository = new EventTableIndexRepository();
        this.tablePerMultiLookup = new HashMap<SubordWMatchExprLookupStrategy, EventTable[]>();
    }

    public EventTableIndexRepository getIndexRepository() {
        return indexRepository;
    }

    public IndexMultiKey[] getIndexes() {
        return indexRepository.getIndexDescriptors();
    }

    public Iterable<EventBean> getDataWindowContents() {
        return dataWindowContents;
    }

    /**
     * Sets the iterator to use to obtain current named window data window contents.
     * @param dataWindowContents iterator over events help by named window
     */
    public void setDataWindowContents(Iterable<EventBean> dataWindowContents)
    {
        this.dataWindowContents = dataWindowContents;
    }

    /**
     * Called by tail view to indicate that the data window view exired events that must be removed from index tables.
     * @param oldData removed stream of the data window
     */
    public void removeOldData(EventBean[] oldData)
    {
        if (rootView.getRevisionProcessor() != null)
        {
            rootView.getRevisionProcessor().removeOldData(oldData, indexRepository);
        }
        else
        {
            for (EventTable table : indexRepository.getTables())
            {
                table.remove(oldData);
            }
        }
    }

    /**
     * Called by tail view to indicate that the data window view has new events that must be added to index tables.
     * @param newData new event
     */
    public void addNewData(EventBean[] newData)
    {
        if (rootView.getRevisionProcessor() == null) {
            // Update indexes for fast deletion, if there are any
            for (EventTable table : indexRepository.getTables())
            {
                table.add(newData);
            }
        }
    }

    // Called by deletion strategy and also the insert-into for new events only
    public void update(EventBean[] newData, EventBean[] oldData)
    {
        if (rootView.getRevisionProcessor() != null)
        {
            rootView.getRevisionProcessor().onUpdate(newData, oldData, this, indexRepository);
        }
        else
        {
            // Update indexes for fast deletion, if there are any
            for (EventTable table : indexRepository.getTables())
            {
                if (rootView.isChildBatching()) {
                    table.add(newData);
                }
            }

            // Update child views
            updateChildren(newData, oldData);
        }
    }

    public void setParent(Viewable parent)
    {
        super.setParent(parent);
    }

    public EventType getEventType()
    {
        return rootView.getEventType();
    }

    public Iterator<EventBean> iterator()
    {
        return null;
    }

    /**
     * Destroy and clear resources.
     */
    public void destroy()
    {
        indexRepository.destroy();
        tablePerMultiLookup.clear();
    }

    /**
     * Return a snapshot using index lookup filters.
     * @param optionalFilter to index lookup
     * @return events
     */
    public Collection<EventBean> snapshot(FilterSpecCompiled optionalFilter, Annotation[] annotations) {
        VirtualDWView virtualDataWindow = null;
        if (isVirtualDataWindow()) {
            virtualDataWindow = getVirtualDataWindow();
        }
        return FireAndForgetQueryExec.snapshot(optionalFilter, annotations, virtualDataWindow,
                indexRepository, rootView.isQueryPlanLogging(), NamedWindowRootView.getQueryPlanLog(),
                rootView.getEventType().getName(), agentInstanceContext);
    }

    /**
     * Add an explicit index.
     *
     * @param unique indicator whether unique
     * @param indexName indexname
     * @param columns properties indexed
     * @throws com.espertech.esper.epl.expression.core.ExprValidationException if the index fails to be valid
     */
    public synchronized void addExplicitIndex(boolean unique, String indexName, List<CreateIndexItem> columns) throws ExprValidationException {
        indexRepository.validateAddExplicitIndex(unique, indexName, columns, rootView.getEventType(), dataWindowContents);
    }

    public boolean isVirtualDataWindow() {
        return this.getViews()[0] instanceof VirtualDWView;
    }

    public VirtualDWView getVirtualDataWindow() {
        if (!isVirtualDataWindow()) {
            return null;
        }
        return (VirtualDWView) this.getViews()[0];
    }

    public void postLoad() {
        EventBean[] events = new EventBean[1];
        for (EventBean event : dataWindowContents) {
            events[0] = event;
            for (EventTable table : indexRepository.getTables()) {
                table.add(events);
            }
        }
    }

    public void visitIndexes(StatementAgentInstancePostLoadIndexVisitor visitor) {
        visitor.visit(indexRepository.getTables());
    }

    public boolean isQueryPlanLogging() {
        return rootView.isQueryPlanLogging();
    }
}

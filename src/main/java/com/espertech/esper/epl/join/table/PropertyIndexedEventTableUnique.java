/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.join.table;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyGetter;
import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.util.*;

public class PropertyIndexedEventTableUnique extends PropertyIndexedEventTable implements EventTableAsSet
{
    protected final Map<MultiKeyUntyped, EventBean> propertyIndex;
    private final boolean canClear;

    public PropertyIndexedEventTableUnique(EventPropertyGetter[] propertyGetters, EventTableOrganization organization) {
        super(propertyGetters, organization);
        propertyIndex = new HashMap<MultiKeyUntyped, EventBean>();
        this.canClear = true;
    }

    public PropertyIndexedEventTableUnique(EventPropertyGetter[] propertyGetters, EventTableOrganization organization, Map<MultiKeyUntyped, EventBean> propertyIndex) {
        super(propertyGetters, organization);
        this.propertyIndex = propertyIndex;
        this.canClear = false;
    }

    /**
     * Remove then add events.
     * @param newData to add
     * @param oldData to remove
     */
    @Override
    public void addRemove(EventBean[] newData, EventBean[] oldData) {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qIndexAddRemove(this, newData, oldData);}
        if (oldData != null) {
            for (EventBean theEvent : oldData) {
                remove(theEvent);
            }
        }
        if (newData != null) {
            for (EventBean theEvent : newData) {
                add(theEvent);
            }
        }
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aIndexAddRemove();}
    }

    /**
     * Add an array of events. Same event instance is not added twice. Event properties should be immutable.
     * Allow null passed instead of an empty array.
     * @param events to add
     * @throws IllegalArgumentException if the event was already existed in the index
     */
    @Override
    public void add(EventBean[] events)
    {
        if (events != null) {

            if (InstrumentationHelper.ENABLED && events.length > 0) {
                InstrumentationHelper.get().qIndexAdd(this, events);
                for (EventBean theEvent : events) {
                    add(theEvent);
                }
                InstrumentationHelper.get().aIndexAdd();
                return;
            }

            for (EventBean theEvent : events) {
                add(theEvent);
            }
        }
    }

    /**
     * Remove events.
     * @param events to be removed, can be null instead of an empty array.
     * @throws IllegalArgumentException when the event could not be removed as its not in the index
     */
    @Override
    public void remove(EventBean[] events)
    {
        if (events != null) {

            if (InstrumentationHelper.ENABLED && events.length > 0) {
                InstrumentationHelper.get().qIndexRemove(this, events);
                for (EventBean theEvent : events) {
                    remove(theEvent);
                }
                InstrumentationHelper.get().aIndexRemove();
                return;
            }

            for (EventBean theEvent : events) {
                remove(theEvent);
            }
        }
    }

    /**
     * Returns the set of events that have the same property value as the given event.
     * @param keys to compare against
     * @return set of events with property value, or null if none found (never returns zero-sized set)
     */
    @Override
    public Set<EventBean> lookup(Object[] keys)
    {
        MultiKeyUntyped key = new MultiKeyUntyped(keys);
        EventBean event = propertyIndex.get(key);
        if (event != null) {
            return Collections.singleton(event);
        }
        return null;
    }

    public void add(EventBean theEvent)
    {
        MultiKeyUntyped key = getMultiKey(theEvent);

        EventBean existing = propertyIndex.put(key, theEvent);
        if (existing != null && !existing.equals(theEvent)) {
            throw handleUniqueIndexViolation(organization.getIndexName(), key);
        }
    }

    protected static EPException handleUniqueIndexViolation(String indexName, Object key) {
        String indexNameDisplay = indexName == null ? "" : " '" + indexName + "'";
        throw new EPException("Unique index violation, index" + indexNameDisplay + " is a unique index and key '" + key + "' already exists");
    }

    public void remove(EventBean theEvent)
    {
        MultiKeyUntyped key = getMultiKey(theEvent);
        propertyIndex.remove(key);
    }

    public boolean isEmpty()
    {
        return propertyIndex.isEmpty();
    }

    @Override
    public Iterator<EventBean> iterator()
    {
        return propertyIndex.values().iterator();
    }

    public void clear()
    {
        if (canClear) {
            propertyIndex.clear();
        }
    }

    @Override
    public Integer getNumberOfEvents() {
        return propertyIndex.size();
    }

    @Override
    public int getNumKeys() {
        return propertyIndex.size();
    }

    @Override
    public Object getIndex() {
        return propertyIndex;
    }

    public String toQueryPlan()
    {
        return this.getClass().getSimpleName() +
                " streamNum=" + organization.getStreamNum() +
                " propertyGetters=" + Arrays.toString(propertyGetters);
    }

    public Set<EventBean> allValues() {
        if (propertyIndex.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<EventBean>(propertyIndex.values());
    }
}

/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.subquery;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.FlushedEventBuffer;
import com.espertech.esper.epl.join.table.EventTable;
import com.espertech.esper.view.internal.BufferObserver;

/**
 * Observer to a buffer that is filled by a subselect view when it posts events,
 * to be added and removed from indexes.
 */
public class SubselectBufferObserver implements BufferObserver
{
    private final EventTable[] eventIndex;

    /**
     * Ctor.
     * @param eventIndex index to update
     */
    public SubselectBufferObserver(EventTable[] eventIndex) {
        this.eventIndex = eventIndex;
    }

    public void newData(int streamId, FlushedEventBuffer newEventBuffer, FlushedEventBuffer oldEventBuffer)
    {
        EventBean[] newData = newEventBuffer.getAndFlush();
        EventBean[] oldData = oldEventBuffer.getAndFlush();
        for (EventTable table : eventIndex) {
            table.addRemove(newData, oldData);
        }
    }
}
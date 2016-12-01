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

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.collection.UniformPair;
import com.espertech.esper.core.service.UpdateDispatchView;
import com.espertech.esper.view.ViewSupport;

import java.util.Iterator;

public class ContextMergeViewForwarding extends ContextMergeView {

    public ContextMergeViewForwarding(EventType eventType) {
        super(eventType);
    }

    @Override
    public void update(EventBean[] newData, EventBean[] oldData) {
        updateChildren(newData, oldData);
    }
}

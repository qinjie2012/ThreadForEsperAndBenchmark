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

import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.filter.FilterValueSetParam;

import java.util.IdentityHashMap;

public class AgentInstanceFilterProxyImpl implements AgentInstanceFilterProxy {

    private final IdentityHashMap<FilterSpecCompiled, FilterValueSetParam[][]> addendumMap;

    public AgentInstanceFilterProxyImpl(IdentityHashMap<FilterSpecCompiled, FilterValueSetParam[][]> addendums) {
        this.addendumMap = addendums;
    }

    public FilterValueSetParam[][] getAddendumFilters(FilterSpecCompiled filterSpec) {
        return addendumMap.get(filterSpec);
    }
}

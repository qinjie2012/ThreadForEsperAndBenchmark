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

import com.espertech.esper.client.context.ContextPartitionIdentifier;

public interface ContextPartitionVisitor {
    public void visit(int nestingLevel,
                      int pathId,
                      ContextStatePathValueBinding binding,
                      Object payload,
                      ContextController contextController,
                      ContextControllerInstanceHandle instanceHandle);
}

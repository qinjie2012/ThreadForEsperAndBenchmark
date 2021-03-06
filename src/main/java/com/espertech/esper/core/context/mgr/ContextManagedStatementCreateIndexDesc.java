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

import com.espertech.esper.core.context.factory.StatementAgentInstanceFactory;
import com.espertech.esper.core.context.util.ContextMergeView;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.spec.StatementSpecCompiled;

public class ContextManagedStatementCreateIndexDesc extends ContextControllerStatementBase {

    public ContextManagedStatementCreateIndexDesc(StatementSpecCompiled statementSpec, StatementContext statementContext, ContextMergeView mergeView, StatementAgentInstanceFactory factory) {
        super(statementSpec, statementContext, mergeView, factory);
    }
}

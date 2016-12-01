/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.core.start;

import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.table.mgmt.TableService;
import com.espertech.esper.util.DestroyCallback;

public class EPStatementDestroyCallbackTableIdxRef implements DestroyCallback
{
    private final TableService tableService;
    private final TableMetadata tableMetadata;
    private final String statementName;

    public EPStatementDestroyCallbackTableIdxRef(TableService tableService, TableMetadata tableMetadata, String statementName) {
        this.tableService = tableService;
        this.tableMetadata = tableMetadata;
        this.statementName = statementName;
    }

    public void destroy() {
        tableService.removeIndexReferencesStmtMayRemoveIndex(statementName, tableMetadata);
    }
}
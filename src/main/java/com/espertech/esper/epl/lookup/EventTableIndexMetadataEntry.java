/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.lookup;

import java.util.HashSet;
import java.util.Set;

public class EventTableIndexMetadataEntry extends EventTableIndexEntryBase
{
    private final boolean primary;
    private final Set<String> referencedByStmt;

    public EventTableIndexMetadataEntry(String optionalIndexName, boolean primary) {
        super(optionalIndexName);
        this.primary = primary;
        referencedByStmt = primary ? null : new HashSet<String>();
    }

    public void addReferringStatement(String statementName) {
        if (!primary) {
            referencedByStmt.add(statementName);
        }
    }

    public boolean removeReferringStatement(String referringStatementName) {
        if (!primary) {
            referencedByStmt.remove(referringStatementName);
            if (referencedByStmt.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public boolean isPrimary() {
        return primary;
    }

    public String[] getReferringStatements() {
        return referencedByStmt.toArray(new String[referencedByStmt.size()]);
    }
}

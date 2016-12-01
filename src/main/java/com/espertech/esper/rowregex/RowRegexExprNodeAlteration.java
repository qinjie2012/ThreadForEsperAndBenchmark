/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.rowregex;

import java.io.StringWriter;

/**
 * Or-condition in a regex expression tree.
 */
public class RowRegexExprNodeAlteration extends RowRegexExprNode
{
    private static final long serialVersionUID = 8383340732689436983L;

    /**
     * Ctor.
     */
    public RowRegexExprNodeAlteration()
    {        
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        String delimiter = "";
        for (RowRegexExprNode node : this.getChildNodes())
        {
            writer.append(delimiter);
            node.toEPL(writer, getPrecedence());
            delimiter = "|";
        }
    }

    public RowRegexExprNodePrecedenceEnum getPrecedence() {
        return RowRegexExprNodePrecedenceEnum.ALTERNATION;
    }
}

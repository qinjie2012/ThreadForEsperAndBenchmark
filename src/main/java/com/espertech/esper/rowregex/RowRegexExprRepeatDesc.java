/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.rowregex;

import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;
import com.espertech.esper.util.MetaDefItem;

import java.io.Serializable;
import java.io.StringWriter;

public class RowRegexExprRepeatDesc implements MetaDefItem, Serializable
{
    private static final long serialVersionUID = -5731091962097679923L;
    private final ExprNode lower;
    private final ExprNode upper;
    private final ExprNode single;

    public RowRegexExprRepeatDesc(ExprNode lower, ExprNode upper, ExprNode single) {
        this.lower = lower;
        this.upper = upper;
        this.single = single;
    }

    public ExprNode getLower() {
        return lower;
    }

    public ExprNode getUpper() {
        return upper;
    }

    public ExprNode getSingle() {
        return single;
    }

    public void toExpressionString(StringWriter writer) {
        writer.write("{");
        if (single != null) {
            writer.write(ExprNodeUtility.toExpressionStringMinPrecedenceSafe(single));
        }
        else {
            if (lower != null) {
                writer.write(ExprNodeUtility.toExpressionStringMinPrecedenceSafe(lower));
            }
            writer.write(",");
            if (upper != null) {
                writer.write(ExprNodeUtility.toExpressionStringMinPrecedenceSafe(upper));
            }
        }
        writer.write("}");
    }
}

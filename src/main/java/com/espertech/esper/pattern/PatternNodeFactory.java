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

package com.espertech.esper.pattern;

import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.spec.FilterSpecRaw;
import com.espertech.esper.epl.spec.PatternGuardSpec;
import com.espertech.esper.epl.spec.PatternObserverSpec;

import java.util.List;

public interface PatternNodeFactory {

    public EvalFactoryNode makeAndNode();
    public EvalFactoryNode makeEveryDistinctNode(List<ExprNode> expressions);
    public EvalFactoryNode makeEveryNode();
    public EvalFactoryNode makeFilterNode(FilterSpecRaw filterSpecification,String eventAsName, Integer consumptionLevel);
    public EvalFactoryNode makeFollowedByNode(List<ExprNode> maxExpressions, boolean hasEngineWideMax);
    public EvalFactoryNode makeGuardNode(PatternGuardSpec patternGuardSpec);
    public EvalFactoryNode makeMatchUntilNode(ExprNode lowerBounds, ExprNode upperBounds, ExprNode singleBounds);
    public EvalFactoryNode makeNotNode();
    public EvalFactoryNode makeObserverNode(PatternObserverSpec patternObserverSpec);
    public EvalFactoryNode makeOrNode();
    public EvalRootFactoryNode makeRootNode(EvalFactoryNode childNode);
}

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

package com.espertech.esper.core.context.activator;

import com.espertech.esper.client.EventType;
import com.espertech.esper.core.service.EPServicesContext;
import com.espertech.esper.core.service.ExprEvaluatorContextStatement;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.named.NamedWindowProcessor;
import com.espertech.esper.epl.property.PropertyEvaluator;
import com.espertech.esper.epl.spec.FilterStreamSpecCompiled;
import com.espertech.esper.epl.spec.StatementSpecCompiled;
import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.metrics.instrumentation.InstrumentationAgent;
import com.espertech.esper.pattern.EvalRootFactoryNode;
import com.espertech.esper.pattern.PatternContext;

import java.lang.annotation.Annotation;
import java.util.List;

public interface ViewableActivatorFactory {
    ViewableActivator createActivatorSimple(FilterStreamSpecCompiled filterStreamSpec);



    ViewableActivator createFilterProxy(EPServicesContext services,
                                               FilterSpecCompiled filterSpec,
                                               Annotation[] annotations,
                                               boolean subselect,
                                               InstrumentationAgent instrumentationAgentSubquery,
                                               boolean isCanIterate);

    ViewableActivator createStreamReuseView(EPServicesContext services,
                                            StatementContext statementContext,
                                            StatementSpecCompiled statementSpec,
                                            FilterStreamSpecCompiled filterStreamSpec,
                                            boolean isJoin,
                                            ExprEvaluatorContextStatement evaluatorContextStmt,
                                            boolean filterSubselectSameStream,
                                            int streamNum,
                                            boolean isCanIterateUnbound);

    ViewableActivator createPattern(PatternContext patternContext,
                                    EvalRootFactoryNode rootFactoryNode,
                                    EventType eventType,
                                    boolean consumingFilters,
                                    boolean suppressSameEventMatches,
                                    boolean discardPartialsOnMatch,
                                    boolean isCanIterateUnbound);

    ViewableActivator createNamedWindow(NamedWindowProcessor processor,
                                        List<ExprNode> filterExpressions,
                                        PropertyEvaluator optPropertyEvaluator);
}

/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.view;

import com.espertech.esper.client.ConfigurationInformation;
import com.espertech.esper.epl.table.mgmt.TableService;
import com.espertech.esper.epl.core.EngineImportService;
import com.espertech.esper.epl.core.MethodResolutionService;
import com.espertech.esper.epl.core.StreamTypeService;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.variable.VariableService;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.schedule.SchedulingService;
import com.espertech.esper.schedule.TimeProvider;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * Interface for views that require validation against stream event types.
 */
public interface ValidatedView
{
    /**
     * Validate the view.
     * @param streamTypeService supplies the types of streams against which to validate
     * @param methodResolutionService for resolving imports and classes and methods
     * @param timeProvider for providing current time
     * @param variableService for access to variables
     * @param exprEvaluatorContext context for expression evaluation
     * @throws ExprValidationException is thrown to indicate an exception in validating the view
     */
    public void validate(EngineImportService engineImportService,
                         StreamTypeService streamTypeService,
                         MethodResolutionService methodResolutionService,
                         TimeProvider timeProvider,
                         VariableService variableService,
                         TableService tableService,
                         ExprEvaluatorContext exprEvaluatorContext,
                         ConfigurationInformation configSnapshot,
                         SchedulingService schedulingService,
                         String engineURI,
                         Map<Integer, List<ExprNode>> sqlParameters,
                         EventAdapterService eventAdapterService,
                         String statementName,
                         String statementId,
                         Annotation[] annotations) throws ExprValidationException;
}

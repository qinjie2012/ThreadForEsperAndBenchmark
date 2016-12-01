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

package com.espertech.esper.core.service.multimatch;

public class MultiMatchHandlerFactory {//多匹配filter处理，为什么不使用线程池去处理
    public static MultiMatchHandler getDefaultHandler() {
        return MultiMatchHandlerSubqueryPreevalNoDedup.INSTANCE;//在类中使用单例模式，只有一个MultiMatchHandlerSubqueryPreevalNoDedup实体
    }
}

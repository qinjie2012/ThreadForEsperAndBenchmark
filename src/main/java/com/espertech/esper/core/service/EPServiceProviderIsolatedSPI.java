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

package com.espertech.esper.core.service;

import com.espertech.esper.client.*;

/**
 * SPU for isolated service provider.
 */
public interface EPServiceProviderIsolatedSPI extends EPServiceProviderIsolated
{
    /**
     * Return isolated services.
     * @return isolated services
     */
    public EPIsolationUnitServices getIsolatedServices();
}
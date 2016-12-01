/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.view;

/**
 * Views that require initialization after view instantiation and after view hook-up with the parent view
 * can impleeent this interface and get invoked to initialize.
 */
public interface InitializableView
{
    /**
     * Initializes a view.
     */
    public void initialize();
}

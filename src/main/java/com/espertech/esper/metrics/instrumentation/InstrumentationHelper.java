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
package com.espertech.esper.metrics.instrumentation;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.util.JavaClassHelper;

public class InstrumentationHelper {
    private final static String PROVIDER_PROPERTY = "instrumentation_provider";

    public final static boolean ENABLED = false;
    public final static boolean ASSERTIONENABLED = false;

    public static Instrumentation DEFAULT_INSTRUMENTATION = new InstrumentationDefault();
    public static Instrumentation instrumentation = DEFAULT_INSTRUMENTATION;

    public static InstrumentationAssertionService assertionService;

    public static Instrumentation get() {
        return instrumentation;
    }

    public static void startTest(EPServiceProvider engine, Class testClass, String testName) {
        if (!ASSERTIONENABLED) {
            return;
        }
        if (assertionService == null) {
            resolveAssertionServie();
        }
        assertionService.startTest(engine, testClass, testName);
    }

    public static void endTest() {
        if (!ASSERTIONENABLED) {
            return;
        }
        assertionService.endTest();
    }

    private static void resolveAssertionServie() {
        String provider = System.getProperty(PROVIDER_PROPERTY);
        if (provider == null) {
            throw new RuntimeException("Failed to find '" + PROVIDER_PROPERTY + "' system property");
        }
        if (provider.toLowerCase().trim().equals("default")) {
            assertionService = new DefaultInstrumentationAssertionService();
        }
        else {
            assertionService = (InstrumentationAssertionService) JavaClassHelper.instantiate(InstrumentationAssertionService.class, provider);
        }
    }

    private static class DefaultInstrumentationAssertionService implements InstrumentationAssertionService {
        public void startTest(EPServiceProvider engine, Class testClass, String testName) {

        }

        public void endTest() {

        }
    }
}

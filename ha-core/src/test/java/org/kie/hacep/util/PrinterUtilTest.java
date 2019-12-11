/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.hacep.util;

import org.junit.Test;
import org.kie.hacep.EnvConfig;
import org.slf4j.Logger;

import static org.junit.Assert.*;

public class PrinterUtilTest {

    @Test
    public void getPrinterTest() {
        EnvConfig config = EnvConfig.getDefaultEnvConfig();
        Printer printer = PrinterUtil.getPrinter(config);
        assertNotNull(printer);
    }

    @Test
    public void getKafkaLoggerTest() {
        EnvConfig config = EnvConfig.getDefaultEnvConfig();
        config.underTest(true);
        config.withPrinterType("org.kie.hacep.util.fake.PrinterLogImpl");
        Logger logger = PrinterUtil.getKafkaLoggerForTest(config);
        assertNotNull(logger);
    }
}
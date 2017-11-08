/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.qpid.util;


import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.test.utils.QpidTestCase;

/**
 *Junit tests for the Serial class 
 */
public class SerialTest extends QpidTestCase
{

    /**
     * Test the key boundaries where wraparound occurs.
     */
    public void testBoundaries()
    {
        assertTrue(Serial.gt(1, 0));
        assertTrue(Serial.lt(0, 1));

        assertTrue(Serial.gt(Integer.MAX_VALUE+1, Integer.MAX_VALUE));
        assertTrue(Serial.lt(Integer.MAX_VALUE, Integer.MAX_VALUE+1));

        assertTrue(Serial.gt(0xFFFFFFFF + 1, 0xFFFFFFFF));
        assertTrue(Serial.lt(0xFFFFFFFF, 0xFFFFFFFF + 1));
    }
}

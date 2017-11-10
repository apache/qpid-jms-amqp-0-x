/*
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

package org.apache.qpid.framing;

import java.nio.ByteBuffer;

import org.apache.qpid.test.utils.QpidTestCase;


public class EncodingUtilsTest extends QpidTestCase
{
    private static final int BUFFER_SIZE = 10;

    private ByteBuffer _buffer;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _buffer = ByteBuffer.allocate(BUFFER_SIZE);
    }

    public void testReadLongAsShortStringWhenDigitsAreSpecified() throws Exception
    {
        _buffer.put((byte)3);
        _buffer.put((byte)'9');
        _buffer.put((byte)'2');
        _buffer.put((byte)'0');
        _buffer.flip();
        assertEquals("Unexpected result", 920L, EncodingUtils.readLongAsShortString(_buffer));
    }

    public void testReadLongAsShortStringWhenNonDigitCharacterIsSpecified() throws Exception
    {
        _buffer.put((byte)2);
        _buffer.put((byte)'1');
        _buffer.put((byte)'a');
        _buffer.flip();
        try
        {
            EncodingUtils.readLongAsShortString(_buffer);
            fail("Exception is expected");
        }
        catch(AMQFrameDecodingException e)
        {
            // pass
        }
    }
}
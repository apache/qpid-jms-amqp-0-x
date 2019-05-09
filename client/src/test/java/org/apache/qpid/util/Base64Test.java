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

import java.nio.charset.StandardCharsets;

import org.apache.qpid.test.utils.QpidTestCase;

public class Base64Test extends QpidTestCase
{

    public void testEncode()
    {
        assertEquals("", Base64.encode("".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zg==", Base64.encode("f".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm8=", Base64.encode("fo".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm9v", Base64.encode("foo".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm9vYg==", Base64.encode("foob".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm9vYmE=", Base64.encode("fooba".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm9vYmFy", Base64.encode("foobar".getBytes(StandardCharsets.ISO_8859_1)));
        assertEquals("Zm9vYmFyMQ==", Base64.encode("foobar1".getBytes(StandardCharsets.ISO_8859_1)));
    }

    public void testDecode()
    {
        assertEquals("", new String(Base64.decode(""), StandardCharsets.ISO_8859_1));
        assertEquals("f",new String(Base64.decode("Zg=="),StandardCharsets.ISO_8859_1));
        assertEquals("fo", new String(Base64.decode("Zm8="), StandardCharsets.ISO_8859_1));
        assertEquals("foo", new String(Base64.decode("Zm9v"), StandardCharsets.ISO_8859_1));
        assertEquals("foob", new String(Base64.decode("Zm9vYg=="), StandardCharsets.ISO_8859_1));
        assertEquals("fooba", new String(Base64.decode("Zm9vYmE="), StandardCharsets.ISO_8859_1));
        assertEquals("foobar", new String(Base64.decode("Zm9vYmFy"), StandardCharsets.ISO_8859_1));
        assertEquals("foobar1", new String(Base64.decode("Zm9vYmFyMQ=="), StandardCharsets.ISO_8859_1));
    }

    public void testDecodeMissingPadding()
    {
        try
        {
                 Base64.decode("Zm9vYmFyMQ=");
                 fail("Exception expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testMalformed()
    {
        try
        {
            Base64.decode("Z=m9vYmFyMQ=");
            fail("Exception expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testUnexpectedPadding()
    {
        try
        {
            Base64.decode("=");
            fail("Exception expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testUnexpectedCharacter()
    {
        try
        {
            Base64.decode("Zm9vYmFyMQ:=");
            fail("Exception expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    public void testMissing()
    {
        try
        {
            Base64.decode("Z");
            fail("Exception expected");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }
}

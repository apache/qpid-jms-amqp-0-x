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
package org.apache.qpid.transport.util;

import static java.lang.Math.min;

import java.nio.ByteBuffer;


/**
 * Functions
 *
 * @author Rafael H. Schloming
 */

public final class Functions
{
    private static final char[] HEX_CHARACTERS =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private Functions()
    {
    }

    public static final int mod(int n, int m)
    {
        int r = n % m;
        return r < 0 ? m + r : r;
    }

    public static final byte lsb(int i)
    {
        return (byte) (0xFF & i);
    }

    public static final byte lsb(long l)
    {
        return (byte) (0xFF & l);
    }

    public static final String str(ByteBuffer buf)
    {
        return str(buf, buf.remaining());
    }

    public static final String str(ByteBuffer buf, int limit)
    {
        return str(buf, limit,buf.position());
    }

    public static final String str(ByteBuffer buf, int limit,int start)
    {
        StringBuilder str = new StringBuilder();
        str.append('"');

        for (int i = start; i < min(buf.limit(), limit); i++)
        {
            byte c = buf.get(i);

            if (c > 31 && c < 127 && c != '\\')
            {
                str.append((char)c);
            }
            else
            {
                str.append(String.format("\\x%02x", c));
            }
        }

        str.append('"');

        if (limit < buf.remaining())
        {
            str.append("...");
        }

        return str.toString();
    }

    public static final String str(byte[] bytes)
    {
        return str(ByteBuffer.wrap(bytes));
    }

    public static final String str(byte[] bytes, int limit)
    {
        return str(ByteBuffer.wrap(bytes), limit);
    }

    public static String hex(byte[] bytes, int limit)
    {
        return hex(bytes, limit, "");
    }

    public static String hex(byte[] bytes, int limit, CharSequence separator)
    {
        limit = Math.min(limit, bytes == null ? 0 : bytes.length);
        StringBuilder sb = new StringBuilder(3 + limit*2);
        for(int i = 0; i < limit; i++)
        {
            sb.append(HEX_CHARACTERS[(((int)bytes[i]) & 0xf0)>>4]);
            sb.append(HEX_CHARACTERS[(((int)bytes[i]) & 0x0f)]);
            if(i != bytes.length - 1)
            {
                sb.append(separator);
            }
        }
        if(bytes != null && bytes.length>limit)
        {
            sb.append("...");
        }
        return sb.toString();
    }
}

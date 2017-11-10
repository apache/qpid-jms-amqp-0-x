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

import java.nio.ByteBuffer;
import java.util.Collection;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

public class ByteBufferUtils
{
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];

    public static ByteBuffer combine(Collection<ByteBuffer> bufs)
    {
        if(bufs == null || bufs.isEmpty())
        {
            return EMPTY_BYTE_BUFFER;
        }
        else
        {
            int size = 0;
            boolean isDirect = false;
            for(ByteBuffer buf : bufs)
            {
                size += buf.remaining();
                isDirect = isDirect || buf.isDirect();
            }
            ByteBuffer combined = isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);

            for(ByteBuffer buf : bufs)
            {
                copyTo(buf, combined);
            }
            combined.flip();
            return combined;
        }
    }


    public static long getUnsignedInt(ByteBuffer buffer)
    {
        return ((long) buffer.getInt()) & 0xffffffffL;
    }

    public static void putUnsignedInt(ByteBuffer buffer, long value)
    {
        buffer.putInt((int) value);
    }

    public static int getUnsignedShort(ByteBuffer buffer)
    {
        return ((int) buffer.getShort()) & 0xffff;
    }

    public static void putUnsignedShort(ByteBuffer buffer, int value)
    {
        buffer.putShort((short) value);
    }

    public static short getUnsignedByte(ByteBuffer buffer)
    {
        return (short) (((short) buffer.get()) & 0xff);
    }

    public static void putUnsignedByte(ByteBuffer buffer, short value)
    {
        buffer.put((byte) value);
    }

    public static ByteBuffer view(ByteBuffer buffer, int offset, int length)
    {
        ByteBuffer view = buffer.slice();
        view.position(offset);
        int newLimit = Math.min(view.position() + length, view.capacity());
        view.limit(newLimit);
        return view.slice();
    }

    public static void copyTo(ByteBuffer src, byte[] dst)
    {
        ByteBuffer copy = src.duplicate();
        copy.get(dst);
    }

    public static void copyTo(ByteBuffer src, ByteBuffer dst)
    {
        ByteBuffer copy = src.duplicate();
        dst.put(copy);
    }

    public static SSLEngineResult encryptSSL(SSLEngine engine,
                                             final Collection<ByteBuffer> buffers,
                                             ByteBuffer dest) throws SSLException
    {
        final ByteBuffer[] src;
        // QPID-7447: prevent unnecessary allocations
        if (buffers.isEmpty())
        {
            src = EMPTY_BYTE_BUFFER_ARRAY;
        }
        else
        {
            src = buffers.toArray(new ByteBuffer[buffers.size()]);
        }
        return engine.wrap(src, dest);
    }
}

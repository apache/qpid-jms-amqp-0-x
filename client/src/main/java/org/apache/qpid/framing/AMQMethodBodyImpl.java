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
package org.apache.qpid.framing;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.util.ByteBufferUtils;

public abstract class AMQMethodBodyImpl implements AMQMethodBody
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQMethodBodyImpl.class);
    public static final byte TYPE = 1;

    public AMQMethodBodyImpl()
    {
    }

    public byte getFrameType()
    {
        return TYPE;
    }


    /** unsigned short
     *
     * @return body size*/
    abstract protected int getBodySize();


    public AMQFrame generateFrame(int channelId)
    {
        return new AMQFrame(channelId, this);
    }

    /**
     * Creates an AMQChannelException for the corresponding body type (a channel exception should include the class and
     * method ids of the body it resulted from).
     */

    public void handle(final int channelId, final AMQVersionAwareProtocolSession session) throws QpidException
    {
        session.methodFrameReceived(channelId, this);
    }

    public int getSize()
    {
        return 2 + 2 + getBodySize();
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {

        final int size = getSize();
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferUtils.putUnsignedShort(buf, getClazz());
        ByteBufferUtils.putUnsignedShort(buf, getMethod());
        writeMethodPayload(buf);
        buf.flip();
        sender.send(buf);
        return size;
    }

    abstract protected void writeMethodPayload(ByteBuffer buffer);


    protected int getSizeOf(AMQShortString string)
    {
        return EncodingUtils.encodedShortStringLength(string);
    }

    protected void writeByte(ByteBuffer buffer, byte b)
    {
        buffer.put(b);
    }

    protected void writeAMQShortString(ByteBuffer buffer, AMQShortString string)
    {
        EncodingUtils.writeShortStringBytes(buffer, string);
    }


    protected void writeInt(ByteBuffer buffer, int i)
    {
        buffer.putInt(i);
    }


    protected int getSizeOf(FieldTable table)
    {
        return EncodingUtils.encodedFieldTableLength(table);  //To change body of created methods use File | Settings | File Templates.
    }

    protected void writeFieldTable(ByteBuffer buffer, FieldTable table)
    {
        EncodingUtils.writeFieldTableBytes(buffer, table);
    }

    protected void writeLong(ByteBuffer buffer, long l)
    {
        buffer.putLong(l);
    }


    protected int getSizeOf(byte[] response)
    {
        return (response == null) ? 4 : response.length + 4;
    }

    protected void writeBytes(ByteBuffer buffer, byte[] data)
    {
        EncodingUtils.writeBytes(buffer,data);
    }

    protected void writeShort(ByteBuffer buffer, short s)
    {
        buffer.putShort(s);
    }

    protected void writeBitfield(ByteBuffer buffer, byte bitfield0)
    {
        buffer.put(bitfield0);
    }

    protected void writeUnsignedShort(ByteBuffer buffer, int s)
    {
        ByteBufferUtils.putUnsignedShort(buffer, s);
    }

    protected void writeUnsignedInteger(ByteBuffer buffer, long i)
    {
        ByteBufferUtils.putUnsignedInt(buffer, i);
    }

    protected void writeUnsignedByte(ByteBuffer buffer, short unsignedByte)
    {
        ByteBufferUtils.putUnsignedByte(buffer, unsignedByte);
    }

}

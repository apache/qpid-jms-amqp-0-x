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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class Base64
{
    private static final char PAD = '=';
    private static final char[] BASE64_ENCODE =
            {
                    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                    'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
            };
    private static final int MASK_8BITS = 0xff;
    private static final int MASK_6BITS = 0x3f;
    private static final int[] BASE64_DECODE =
            {
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54,
                    55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2,
                    3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                    20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30,
                    31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
                    48, 49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
            };

    public static String encode(byte[] data)
    {
        final StringBuilder encoded = new StringBuilder(4 * ((data.length + 2) / 3));
        final int triples = data.length / 3 * 3;
        int i = 0;
        for (; i < triples; i += 3)
        {
            final int bits =
                    (data[i] & MASK_8BITS) << 16 | (data[i + 1] & MASK_8BITS) << 8 | (data[i + 2] & MASK_8BITS);
            encoded.append(BASE64_ENCODE[(bits >>> 18) & MASK_6BITS]);
            encoded.append(BASE64_ENCODE[(bits >>> 12) & MASK_6BITS]);
            encoded.append(BASE64_ENCODE[(bits >>> 6) & MASK_6BITS]);
            encoded.append(BASE64_ENCODE[bits & MASK_6BITS]);
        }

        if (i < data.length)
        {
            final int remaining1 = data[i] & MASK_8BITS;
            encoded.append(BASE64_ENCODE[remaining1 >> 2]);
            if (i == data.length - 1)
            {
                encoded.append(BASE64_ENCODE[(remaining1 << 4) & MASK_6BITS]);
                encoded.append(PAD);
                encoded.append(PAD);
            }
            else
            {
                final int remaining2 = data[i + 1] & 0xff;
                encoded.append(BASE64_ENCODE[(remaining1 << 4) & MASK_6BITS | (remaining2 >> 4)]);
                encoded.append(BASE64_ENCODE[(remaining2 << 2) & MASK_6BITS]);
                encoded.append(PAD);
            }
        }

        return encoded.toString();
    }

    public static byte[] decode(String data)
    {
        final byte[] bytes = data.getBytes(StandardCharsets.ISO_8859_1);
        final int decodeSize =
                3 * ((bytes.length + 3) / 4) - ((bytes.length & 0x3) != 0 ? 4 - (bytes.length & 0x3) : 0);
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(decodeSize);

        for (int i = 0; i < bytes.length; i += 4)
        {
            int b = 0;
            int byteCounter = 0;
            if (check(bytes, i, byteCounter))
            {
                b = get(bytes, i, byteCounter)  << 18;
                byteCounter++;
                if (check(bytes, i, byteCounter))
                {
                    b = b | (get(bytes, i, byteCounter) << 12);
                    byteCounter++;
                    if (check(bytes, i, byteCounter))
                    {
                        b = b | (get(bytes, i, byteCounter) << 6);
                        byteCounter++;
                        if (check(bytes, i, byteCounter))
                        {
                            b = b | get(bytes, i, byteCounter);
                            byteCounter++;
                        }
                    }
                }
            }

            if (byteCounter == 4)
            {
                buffer.write((byte) (b >> 16) & MASK_8BITS);
                buffer.write((byte) (b >> 8) & MASK_8BITS);
                buffer.write((byte) (b) & MASK_8BITS);
            }
            else if (byteCounter == 3)
            {
                buffer.write((byte) (b >> 16) & MASK_8BITS);
                buffer.write((byte) (b >> 8 ) & MASK_8BITS);
            }
            else if (byteCounter == 2)
            {
                buffer.write((byte) (b >> 16) & MASK_8BITS);
            }
            else
            {
                throw new IllegalArgumentException("Malformed data");
            }
        }
        return buffer.toByteArray();
    }

    private static int get(byte[] bytes, int index, int shift)
    {
        final int i = index + shift;
        if (i>= 0 && i < bytes.length)
        {
            int b = bytes[i] & MASK_8BITS;
            if (b == PAD)
            {
                throw new IllegalArgumentException(String.format("Unexpected padding detected at position %d", i));
            }
            int d = BASE64_DECODE[b];
            if (d < 0)
            {
                throw new IllegalArgumentException(String.format("Unexpected character '%x' at position %d", b, i));
            }
            return d & MASK_8BITS;
        }
        throw new IllegalArgumentException("Unexpected index");
    }

    private static boolean check(byte[] bytes, int index, int shift)
    {
        final int i = index + shift;
        if (i < bytes.length)
        {
            final int b = bytes[i] & MASK_8BITS;
            if (b == PAD)
            {
                if (shift == 0)
                {
                    throw new IllegalArgumentException(String.format("Unexpected padding detected at position %d", i));
                }
                else if (shift == 2
                         && ((i == bytes.length - 2 && bytes[i + 1] != PAD)
                             || (i == bytes.length - 1 && bytes[i - 1] != PAD)))
                {
                    throw new IllegalArgumentException("Missing padding");
                }
                else if (shift != 2 && i < bytes.length - 1)
                {
                    throw new IllegalArgumentException(String.format("Unexpected padding detected at position %d", i));
                }
                return false;
            }
            else if (BASE64_DECODE[b] < 0)
            {
                throw new IllegalArgumentException(String.format("Unexpected character '%x' at position %d", b, i));
            }
            return true;
        }
        return false;
    }

}

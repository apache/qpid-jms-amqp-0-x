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
package org.apache.qpid.systest.consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.junit.Test;

import org.apache.qpid.systest.core.JmsTestBase;

public class BlockingReceiveTest extends JmsTestBase
{
    @Test
    public void testReceiveReturnsNull() throws Exception
    {
        final Connection connection =  getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(getTestQueueName());
            MessageConsumer consumer = session.createConsumer(destination);
            connection.start();

            Runnable r = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(1000);
                        connection.close();
                    }
                    catch (Exception e)
                    {
                    }
                }
            };
            long startTime = System.currentTimeMillis();
            Thread thread = new Thread(r);
            thread.start();
            try
            {
                Message receive = consumer.receive(10000);
                assertTrue(System.currentTimeMillis() - startTime < 10000);
                assertNull(receive);
            }
            finally
            {
                thread.join();
            }
        }
        finally
        {
            connection.close();
        }
    }
}

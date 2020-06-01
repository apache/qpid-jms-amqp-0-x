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
package org.apache.qpid.systest.connection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.util.SystemUtils;

public class FailoverMethodTest extends JmsTestBase implements ExceptionListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverMethodTest.class);
    private final SettableFuture<JMSException> _failoverComplete = SettableFuture.create();
    private int _freePortWithNoBroker;
    private int _port;
    private DummyServer _dummyServer;

    @Before
    public void setUp() throws Exception
    {
        assumeThat("Test requires redevelopment - timings/behaviour on windows mean it fails",
                   SystemUtils.isWindows(), is(not(true)));

        _dummyServer = new DummyServer();
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        _port = brokerAddress.getPort();
        _freePortWithNoBroker = _dummyServer.start();
    }

    @After
    public void tearDown()
    {
        _dummyServer.stop();
    }
    /**
     * Test that the round robin method has the correct delays.
     * The first connection will work but the localhost connection should fail but the duration it takes
     * to report the failure is what is being tested.
     *
     */
    @Test
    public void testFailoverRoundRobinDelay() throws Exception
    {
        //note: The first broker has no connect delay and the default 1 retry
        //        while the tcp:localhost broker has 3 retries with a 2s connect delay
        String connectionString = String.format(
                "amqp://%s:%s@/?brokerlist='tcp://localhost:%d;tcp://localhost:%d?connectdelay='2000',retries='3''",
                getBrokerAdmin().getValidUsername(),
                getBrokerAdmin().getValidPassword(),
                _port,
                _freePortWithNoBroker);

        AMQConnectionURL url = new AMQConnectionURL(connectionString);

        AMQConnection connection = null;
        try
        {
            long start = System.currentTimeMillis();
            connection = new AMQConnection(url);

            connection.setExceptionListener(this);

            LOGGER.debug("Stopping broker");
            getBrokerAdmin().stop();
            LOGGER.debug("Stopped broker");

            _failoverComplete.get(30, TimeUnit.SECONDS);

            long end = System.currentTimeMillis();

            long duration = (end - start);

            //Failover should take more that 6 seconds.
            // 3 Retries
            // so VM Broker NoDelay 0 (Connect) NoDelay 0
            // then TCP NoDelay 0 Delay 1 Delay 2 Delay  3
            // so 3 delays of 2s in total for connection
            // as this is a tcp connection it will take 1second per connection to fail
            // so max time is 6seconds of delay plus 4 seconds of TCP Delay + 1 second of runtime. == 11 seconds

            // Ensure we actually had the delay
            assertTrue("Failover took less than 6 seconds", duration > 6000);

            // Ensure we don't have delays before initial connection and reconnection.
            // We allow 1 second for initial connection and failover logic on top of 6s of sleep.
            assertTrue("Failover took more than 11 seconds:(" + duration + ")", duration < 11000);
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    @Test
    public void testFailoverSingleDelay() throws Exception
    {
        String connectionString = String.format(
                "amqp://%s:%s@/?brokerlist='tcp://localhost:%d?connectdelay='2000',retries='3''",
                getBrokerAdmin().getValidUsername(),
                getBrokerAdmin().getValidPassword(),
                _port);

        AMQConnectionURL url = new AMQConnectionURL(connectionString);
        AMQConnection connection = null;
        try
        {
            long start = System.currentTimeMillis();
            connection = new AMQConnection(url);

            connection.setExceptionListener(this);

            LOGGER.debug("Stopping broker");
            getBrokerAdmin().stop();
            LOGGER.debug("Stopped broker");

            _failoverComplete.get(30, TimeUnit.SECONDS);

            long end = System.currentTimeMillis();

            long duration = (end - start);

            //Failover should take more that 6 seconds.
            // 3 Retries
            // so NoDelay 0 (Connect) NoDelay 0 Delay 1 Delay 2 Delay  3
            // so 3 delays of 2s in total for connection
            // so max time is 6 seconds of delay + 1 second of runtime. == 7 seconds

            // Ensure we actually had the delay
            assertTrue("Failover took less than 6 seconds", duration > 6000);

            // Ensure we don't have delays before initial connection and reconnection.
            // We allow 3 second for initial connection and failover logic on top of 6s of sleep.
            assertTrue("Failover took more than 9 seconds:(" + duration + ")", duration < 9000);
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }
    }

    @Override
    public void onException(JMSException e)
    {
        _failoverComplete.set(e);
    }

    class DummyServer implements Runnable
    {
        private ExecutorService _executorService;
        private ServerSocket _serverSocket;
        private boolean _started;

        synchronized int start() throws IOException
        {
            if (!_started)
            {
                _executorService = Executors.newSingleThreadExecutor();
                _serverSocket = new ServerSocket(0);
                _started = true;
                _executorService.submit(this);
                return _serverSocket.getLocalPort();
            }
            return -1;
        }

        synchronized void stop()
        {
            if (_started)
            {
                _started = false;
                closeSafely();
            }
        }

        public void run()
        {
            while (_started)
            {
                try
                {
                    acceptAndClose();
                }
                catch (IOException e)
                {
                    LOGGER.warn("Failed to close client socket", e);
                    stop();
                }
            }
        }

        private void acceptAndClose() throws IOException
        {
            final Socket socket = _serverSocket.accept();
            socket.close();
        }

        private synchronized void closeSafely()
        {
            try
            {
                _serverSocket.close();
            }
            catch (IOException e)
            {
                LOGGER.warn("Failed to close server socket", e);
            }
            finally
            {
                _executorService.shutdown();
            }
        }
    }

}

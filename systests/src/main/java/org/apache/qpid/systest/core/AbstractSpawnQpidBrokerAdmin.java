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

package org.apache.qpid.systest.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systest.core.logback.LogbackPropertyValueDiscriminator;
import org.apache.qpid.systest.core.util.FileUtils;
import org.apache.qpid.systest.core.util.SystemUtils;

public abstract class AbstractSpawnQpidBrokerAdmin implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSpawnQpidBrokerAdmin.class);

    protected static final String SYSTEST_PROPERTY_BROKER_READY_LOG = "qpid.systest.broker.ready";
    protected static final String SYSTEST_PROPERTY_BROKER_STOPPED_LOG = "qpid.systest.broker.stopped";
    protected static final String SYSTEST_PROPERTY_BROKER_LISTENING_LOG = "qpid.systest.broker.listening";
    protected static final String SYSTEST_PROPERTY_BROKER_PROCESS_LOG = "qpid.systest.broker.process";

    private static final String SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME = "qpid.systest.broker_startup_time";
    private static final String SYSTEST_PROPERTY_BROKER_CLEAN_BETWEEN_TESTS = "qpid.systest.broker.clean.between.tests";

    private volatile List<ListeningPort> _ports;
    private volatile Process _process;
    private volatile Integer _pid;
    private volatile String _currentWorkDirectory;
    private ExecutorService _executorService;
    private Class _currentTestClass;
    private Method _currentTestMethod;

    @Override
    public void beforeTestClass(final Class testClass)
    {
        _currentTestClass = testClass;
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= creating broker for test class : {}", testClass.getSimpleName());
        setUp(testClass);
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        _currentTestMethod = method;
        begin(testClass, method);
        LOGGER.info("========================= executing test : {}#{}", testClass.getSimpleName(), method.getName());
        setClassQualifiedTestName(testClass, method);
        LOGGER.info("========================= start executing test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        _currentTestMethod = null;
        LOGGER.info("========================= stop executing test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        setClassQualifiedTestName(testClass, null);
        LOGGER.info("========================= cleaning up test environment for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        end(testClass, method);
        LOGGER.info("========================= cleaning done for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        _currentTestClass = null;
        LOGGER.info("========================= stopping broker for test class: {}", testClass.getSimpleName());
        cleanUp(testClass);
        LOGGER.info("========================= stopping broker done for test class : {}", testClass.getSimpleName());
        setClassQualifiedTestName(null);
    }

    @Override
    public void restart()
    {
        end(_currentTestClass, _currentTestMethod);
        begin(_currentTestClass, _currentTestMethod);
    }

    @Override
    public void stop()
    {
        end(_currentTestClass, _currentTestMethod);
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        Integer port = null;
        switch (portType)
        {
            case AMQP:
                for (ListeningPort p : _ports)
                {
                    if (p.getTransport().contains("TCP"))
                    {
                        port = p.getPort();
                        break;
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown port type '%s'", portType));
        }
        if (port == null)
        {
            throw new IllegalArgumentException(String.format("Cannot find port of type '%s'", portType));
        }
        return new InetSocketAddress(port);
    }

    @Override
    public Connection getConnection() throws JMSException
    {
        return createConnection(getVirtualHostName(), null);
    }

    @Override
    public Connection getConnection(final Map<String, String> options) throws JMSException
    {
        return createConnection(getVirtualHostName(), options);
    }

    protected abstract void setUp(final Class testClass);

    protected abstract void cleanUp(final Class testClass);

    protected abstract void begin(final Class testClass, final Method method);

    protected abstract void end(final Class testClass, final Method method);

    protected abstract ProcessBuilder createBrokerProcessBuilder(final String workDirectory, final Class testClass) throws IOException;


    protected void runBroker(final Class testClass,
                             final Method method,
                             final String readyLogPattern,
                             final String stopLogPattern,
                             final String portListeningLogPattern,
                             final String processPIDLogPattern) throws IOException
    {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
        String test = testClass.getSimpleName();
        if (method != null)
        {
            test += "-" + method.getName();
        }
        _currentWorkDirectory =
                Files.createTempDirectory(String.format("qpid-work-%s-%s-", timestamp, test))
                     .toString();

        LOGGER.debug("Spawning broker working folder: {}", _currentWorkDirectory);

        int startUpTime = Integer.getInteger(SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME, 30000);

        LOGGER.debug("Spawning broker permitted start-up time: {}", startUpTime);

        ProcessBuilder processBuilder = createBrokerProcessBuilder(_currentWorkDirectory, testClass);
        processBuilder.redirectErrorStream(true);

        Map<String, String> processEnvironment = processBuilder.environment();
        processEnvironment.put("QPID_PNAME", String.format("-DPNAME=QPBRKR -DTNAME=\"%s\"",  testClass.getName()));

        CountDownLatch readyLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();

        LOGGER.debug("Starting broker process");
        _process = processBuilder.start();

        BrokerSystemOutputHandler brokerSystemOutputHandler = new BrokerSystemOutputHandler(_process.getInputStream(),
                                                                                            readyLogPattern,
                                                                                            stopLogPattern,
                                                                                            processPIDLogPattern,
                                                                                            portListeningLogPattern,
                                                                                            readyLatch,
                                                                                            getClass().getName()
        );
        boolean brokerStarted = false;
        _executorService = Executors.newFixedThreadPool(1, new ThreadFactory()
        {
            @Override
            public Thread newThread(final Runnable r)
            {
                Thread t = new Thread(r, BrokerSystemOutputHandler.class.getSimpleName());
                t.setDaemon(false);
                return t;
            }
        });
        try
        {
            _executorService.submit(brokerSystemOutputHandler);
            if (!readyLatch.await(startUpTime, TimeUnit.MILLISECONDS))
            {
                LOGGER.warn("Spawned broker failed to become ready within {} ms. Ready line '{}'",
                            startUpTime, readyLogPattern);
                throw new BrokerAdminException(String.format(
                        "Broker failed to become ready within %d ms. Stop line : %s",
                        startUpTime,
                        readyLogPattern));
            }

            _pid = brokerSystemOutputHandler.getPID();
            _ports = brokerSystemOutputHandler.getAmqpPorts();

            if (_pid == -1)
            {
                throw new BrokerAdminException("Broker PID is not detected");
            }

            if (_ports.size() == 0)
            {
                throw new BrokerAdminException("Broker port is not detected");
            }

            try
            {
                //test that the broker is still running and hasn't exited unexpectedly
                int exit = _process.exitValue();
                LOGGER.info("broker aborted: {}", exit);
                throw new BrokerAdminException("broker aborted: " + exit);
            }
            catch (IllegalThreadStateException e)
            {
                // this is expect if the broker started successfully
            }

            LOGGER.info("Broker was started successfully within {} milliseconds, broker PID {}",
                        System.currentTimeMillis() - startTime,
                        _pid);
            LOGGER.info("Broker ports: {}", _ports);
            brokerStarted = true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (InterruptedException e)
        {
            Thread.interrupted();
        }
        catch (Exception e)
        {
            throw new BrokerAdminException(String.format("Unexpected exception on broker startup: %s", e), e);
        }
        finally
        {
            if (!brokerStarted)
            {
                LOGGER.warn("Broker failed to start");
                _process.destroy();
                _process = null;
                _executorService.shutdown();
                _executorService = null;
                _ports = null;
                _pid = null;
            }
        }
    }

    protected void shutdownBroker()
    {
        try
        {
            if (SystemUtils.isWindows())
            {
                doWindowsKill();
            }

            if (_process != null)
            {
                LOGGER.info("Destroying broker process");
                _process.destroy();

                reapChildProcess();
            }
        }
        finally
        {
            if (_executorService != null)
            {
                _executorService.shutdown();
            }
            if (_ports != null)
            {
                _ports.clear();
                _ports = null;
            }
            _pid = null;
            _process = null;
            if (_currentWorkDirectory != null && Boolean.getBoolean(SYSTEST_PROPERTY_BROKER_CLEAN_BETWEEN_TESTS))
            {
                if (FileUtils.delete(new File(_currentWorkDirectory), true))
                {
                    _currentWorkDirectory = null;
                }
            }
        }
    }

    protected String escapePath(String value)
    {
        if (SystemUtils.isWindows() && value.contains("\"") && !value.startsWith("\""))
        {
            return "\"" + value.replaceAll("\"", "\"\"") + "\"";
        }
        else
        {
            return value;
        }
    }

    protected Connection createConnection(final String virtualHostName,
                                        final Map<String, String> options) throws JMSException
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY,
                                      "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        String urlTemplate = "amqp://:@%s/%s?brokerlist='tcp://localhost:%d?failover='nofailover''";
        StringBuilder url = new StringBuilder(String.format(urlTemplate,
                                                            "spawn_broker_admin",
                                                            virtualHostName,
                                                            getBrokerAddress(PortType.AMQP).getPort()));
        if (options != null)
        {
            for (Map.Entry<String, String> option : options.entrySet())
            {
                url.append("&").append(option.getKey()).append("='").append(option.getValue()).append("'");
            }
        }
        initialContextEnvironment.put("connectionfactory." + factoryName, url.toString());
        try
        {
            InitialContext initialContext = new InitialContext(initialContextEnvironment);
            try
            {
                ConnectionFactory factory = (ConnectionFactory) initialContext.lookup(factoryName);
                return factory.createConnection(getValidUsername(), getValidPassword());
            }
            finally
            {
                initialContext.close();
            }
        }
        catch (NamingException e)
        {
            throw new BrokerAdminException("Unexpected exception on connection lookup", e);
        }
    }

    protected void setClassQualifiedTestName(final Class testClass, final Method method)
    {
        String qualifiedTestName = null;
        if (testClass != null)
        {
            if (method == null)
            {
                qualifiedTestName = testClass.getName();
            }
            else
            {
                qualifiedTestName = String.format("%s.%s", testClass.getName(), method.getName());
            }
        }
        setClassQualifiedTestName(qualifiedTestName);
    }

    private void setClassQualifiedTestName(final String qualifiedTestName)
    {
        final LoggerContext loggerContext = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();
        loggerContext.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, qualifiedTestName);
    }

    private void doWindowsKill()
    {
        try
        {

            Process p;
            p = Runtime.getRuntime().exec(new String[]{"taskkill", "/PID", Integer.toString(_pid), "/T", "/F"});
            consumeAllOutput(p);
        }
        catch (IOException e)
        {
            LOGGER.error("Error whilst killing process " + _pid, e);
        }
    }

    private static void consumeAllOutput(Process p) throws IOException
    {
        try (InputStreamReader inputStreamReader = new InputStreamReader(p.getInputStream()))
        {
            try (BufferedReader reader = new BufferedReader(inputStreamReader))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    LOGGER.debug("Consuming output: {}", line);
                }
            }
        }
    }

    private void reapChildProcess()
    {
        try
        {
            _process.waitFor();
            LOGGER.info("broker exited: " + _process.exitValue());
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Interrupted whilst waiting for process shutdown");
            Thread.currentThread().interrupt();
        }
        finally
        {
            try
            {
                _process.getInputStream().close();
                _process.getErrorStream().close();
                _process.getOutputStream().close();
            }
            catch (IOException ignored)
            {
            }
        }
    }

    private final class BrokerSystemOutputHandler implements Runnable
    {
        private final BufferedReader _in;
        private final List<ListeningPort> _amqpPorts;
        private final Logger _out;
        private final Pattern _readyPattern;
        private final Pattern _stoppedPattern;
        private final Pattern _pidPattern;
        private final Pattern _amqpPortPattern;
        private final CountDownLatch _readyLatch;

        private volatile boolean _seenReady;
        private volatile int _pid;

        private BrokerSystemOutputHandler(InputStream in,
                                          String readyRegExp,
                                          String stoppedRedExp,
                                          String pidRegExp,
                                          String amqpPortRegExp,
                                          CountDownLatch readyLatch,
                                          String loggerName)
        {
            _amqpPorts = new ArrayList<>();
            _seenReady = false;
            _in = new BufferedReader(new InputStreamReader(in));
            _out = LoggerFactory.getLogger(loggerName);
            _readyPattern = Pattern.compile(readyRegExp);
            _stoppedPattern = Pattern.compile(stoppedRedExp);
            _amqpPortPattern = Pattern.compile(amqpPortRegExp);
            _pidPattern = Pattern.compile(pidRegExp);
            _readyLatch = readyLatch;
        }

        @Override
        public void run()
        {
            try
            {
                String line;
                while ((line = _in.readLine()) != null)
                {
                    _out.info(line);

                    checkPortListeningLog(line, _amqpPortPattern, _amqpPorts);

                    Matcher pidMatcher = _pidPattern.matcher(line);
                    if (pidMatcher.find())
                    {
                        if (pidMatcher.groupCount() > 1)
                        {
                            _pid = Integer.parseInt(pidMatcher.group(1));
                        }
                    }

                    Matcher readyMatcher = _readyPattern.matcher(line);
                    if (readyMatcher.find())
                    {
                        _seenReady = true;
                        _readyLatch.countDown();
                    }

                    if (!_seenReady)
                    {
                        Matcher stopMatcher = _stoppedPattern.matcher(line);
                        if (stopMatcher.find())
                        {
                            break;
                        }
                    }
                }
            }
            catch (IOException e)
            {
                LOGGER.warn(e.getMessage()
                            + " : Broker stream from unexpectedly closed; last log lines written by Broker may be lost.");
            }
        }

        private void checkPortListeningLog(final String line,
                                           final Pattern portPattern,
                                           final List<ListeningPort> ports)
        {
            Matcher portMatcher = portPattern.matcher(line);
            if (portMatcher.find())
            {
                ports.add(new ListeningPort(portMatcher.group(1),
                                            Integer.parseInt(portMatcher.group(2))));
            }
        }

        int getPID()
        {
            return _pid;
        }

        List<ListeningPort> getAmqpPorts()
        {
            return _amqpPorts;
        }
    }

    private static class ListeningPort
    {
        private String _transport;
        private int _port;

        ListeningPort(final String transport, final int port)
        {
            _transport = transport;
            _port = port;
        }

        String getTransport()
        {
            return _transport;
        }

        int getPort()
        {
            return _port;
        }

        @Override
        public String toString()
        {
            return "ListeningPort{" +
                   ", _transport='" + _transport + '\'' +
                   ", _port=" + _port +
                   '}';
        }
    }
}

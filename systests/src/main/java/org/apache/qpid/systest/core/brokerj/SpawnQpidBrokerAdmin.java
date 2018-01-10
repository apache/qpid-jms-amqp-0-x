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

package org.apache.qpid.systest.core.brokerj;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.BrokerAdminException;
import org.apache.qpid.systest.core.dependency.ClasspathQuery;
import org.apache.qpid.systest.core.logback.LogbackPropertyValueDiscriminator;
import org.apache.qpid.systest.core.util.FileUtils;
import org.apache.qpid.systest.core.logback.LogbackSocketPortNumberDefiner;
import org.apache.qpid.systest.core.util.SystemUtils;

public class SpawnQpidBrokerAdmin implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpawnQpidBrokerAdmin.class);
    private static final String BROKER_LOG_PREFIX = "BROKER";
    private static final String SYSTEST_PROPERTY_PREFIX = "qpid.systest.";
    private static final String SYSTEST_PROPERTY_BROKER_READY = "qpid.systest.broker.ready";
    private static final String SYSTEST_PROPERTY_BROKER_STOPPED = "qpid.systest.broker.stopped";
    private static final String SYSTEST_PROPERTY_BROKER_LISTENING = "qpid.systest.broker.listening";
    private static final String SYSTEST_PROPERTY_BROKER_PROCESS = "qpid.systest.broker.process";
    private static final String SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME = "qpid.systest.broker_startup_time";
    private static final String SYSTEST_PROPERTY_BROKER_CLEAN_BETWEEN_TESTS = "qpid.systest.broker.clean.between.tests";

    static final String SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE = "qpid.systest.virtualhostnode.type";
    static final String SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT = "qpid.systest.virtualhost.blueprint";
    static final String SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION = "qpid.systest.initialConfigurationLocation";
    static final String SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE = "qpid.systest.build.classpath.file";
    static final String SYSTEST_PROPERTY_JAVA8_EXECUTABLE = "qpid.systest.java8.executable";
    static final String SYSTEST_PROPERTY_BROKERJ_DEPENDECIES = "qpid.systest.brokerj.dependencies";

    private final static AtomicLong BROKER_INSTANCE_COUNTER = new AtomicLong();

    private volatile List<ListeningPort> _ports;
    private volatile Process _process;
    private volatile Integer _pid;
    private volatile String _currentWorkDirectory;
    private volatile boolean _isPersistentStore;
    private volatile String _virtualHostNodeName;

    @Override
    public void create(final Class testClass)
    {
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= starting broker for test class : {}", testClass.getSimpleName());
        startBroker(testClass);
    }

    @Override
    public void start(final Class testClass, final Method method)
    {
        LOGGER.info("========================= prepare test environment for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        String virtualHostNodeName = getVirtualHostNodeName(testClass, method);
        createVirtualHost(virtualHostNodeName);
        _virtualHostNodeName = virtualHostNodeName;
        LOGGER.info("========================= executing test : {}#{}", testClass.getSimpleName(), method.getName());
        setClassQualifiedTestName(testClass.getName() + "." + method.getName());
        LOGGER.info("========================= start executing test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
    }


    @Override
    public void stop(final Class testClass, final Method method)
    {
        LOGGER.info("========================= stop executing test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        setClassQualifiedTestName(testClass.getName());
        LOGGER.info("========================= cleaning up test environment for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
        deleteVirtualHost(getVirtualHostNodeName(testClass, method));
        _virtualHostNodeName = null;
        LOGGER.info("========================= cleaning done for test : {}#{}",
                    testClass.getSimpleName(),
                    method.getName());
    }

    @Override
    public void destroy(final Class testClass)
    {
        LOGGER.info("========================= stopping broker for test class: {}", testClass.getSimpleName());
        shutdown();
        _ports.clear();
        if (Boolean.getBoolean(SYSTEST_PROPERTY_BROKER_CLEAN_BETWEEN_TESTS))
        {
            FileUtils.delete(new File(_currentWorkDirectory), true);
        }
        _isPersistentStore = false;
        LOGGER.info("========================= stopping broker done for test class : {}", testClass.getSimpleName());
        setClassQualifiedTestName(null);
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
                    if (p.getProtocol() == null
                        && (p.getTransport().contains("TCP") /*|| p.getTransport().contains("SSL") */))
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
    public boolean supportsPersistence()
    {
        return _isPersistentStore;
    }

    @Override
    public ListenableFuture<Void> restart()
    {
        if (_virtualHostNodeName == null)
        {
            throw new BrokerAdminException("Virtual host is not started");
        }
        return restartVirtualHost(_virtualHostNodeName);
    }

    @Override
    public String getValidUsername()
    {
        return "guest";
    }

    @Override
    public String getValidPassword()
    {
        return "guest";
    }

    @Override
    public String getType()
    {
        return SpawnQpidBrokerAdmin.class.getSimpleName();
    }

    @Override
    public BrokerType getBrokerType()
    {
        return BrokerType.BROKERJ;
    }

    @Override
    public Connection getConnection() throws JMSException
    {
        return createConnection(_virtualHostNodeName);
    }

    private void startBroker(final Class testClass)
    {
        try
        {
            start(testClass);
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            else
            {
                throw new BrokerAdminException("Unexpected exception on broker startup", e);
            }
        }
    }

    void start(final Class testClass) throws Exception
    {
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
        _currentWorkDirectory =
                Files.createTempDirectory(String.format("qpid-work-%s-%s-", timestamp, testClass.getSimpleName()))
                     .toString();

        String initialConfiguration = System.getProperty(SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION);
        if (initialConfiguration == null)
        {
            throw new BrokerAdminException(
                    String.format("No initial configuration is found: JVM property '%s' is not set.",
                                  SYSTEST_PROPERTY_INITIAL_CONFIGURATION_LOCATION));
        }

        File testInitialConfiguration = new File(_currentWorkDirectory, "initial-configuration.json");
        if (!testInitialConfiguration.createNewFile())
        {
            throw new BrokerAdminException("Failed to create a file for a copy of initial configuration");
        }
        if (initialConfiguration.startsWith("classpath:"))
        {
            String config = initialConfiguration.substring("classpath:".length());
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(config);
                 OutputStream os = new FileOutputStream(testInitialConfiguration))
            {
                ByteStreams.copy(is, os);
            }
        }
        else
        {
            Files.copy(new File(initialConfiguration).toPath(), testInitialConfiguration.toPath());
        }

        String classpath;
        File file = new File(System.getProperty(SYSTEST_PROPERTY_BUILD_CLASSPATH_FILE));
        if (!file.exists())
        {
            String dependencies = System.getProperty(SYSTEST_PROPERTY_BROKERJ_DEPENDECIES);
            final ClasspathQuery classpathQuery = new ClasspathQuery(SpawnQpidBrokerAdmin.class,
                                                                     Arrays.asList(dependencies.split(",")));
            classpath = classpathQuery.getClasspath();
            Files.write(file.toPath(), Collections.singleton(classpath), UTF_8);
        }
        else
        {
            classpath = new String(Files.readAllBytes(file.toPath()), UTF_8);
        }

        // grab Qpid related JVM settings
        List<String> jvmArguments = new ArrayList<>();
        Properties jvmProperties = System.getProperties();
        for (String jvmProperty : jvmProperties.stringPropertyNames())
        {
            if (jvmProperty.startsWith(SYSTEST_PROPERTY_PREFIX)
                || jvmProperty.equalsIgnoreCase("java.io.tmpdir"))
            {
                jvmArguments.add("-D" + jvmProperty + "=" + jvmProperties.getProperty(jvmProperty));
            }
        }

        jvmArguments.add(0, System.getProperty(SYSTEST_PROPERTY_JAVA8_EXECUTABLE, "/usr/bin/java"));
        jvmArguments.add(1, "-cp");
        jvmArguments.add(2, classpath);
        jvmArguments.add("-Dqpid.systest.logback.socket.port="
                         + LogbackSocketPortNumberDefiner.getLogbackSocketPortNumber());
        jvmArguments.add("-Dqpid.systest.logback.logs_dir=" + System.getProperty("qpid.systest.logback.logs_dir",
                                                                                 "${qpid.work_dir}"));
        jvmArguments.add(String.format("-Dqpid.systest.logback.origin=%s-%d",
                                       BROKER_LOG_PREFIX,
                                       BROKER_INSTANCE_COUNTER.getAndIncrement()));
        jvmArguments.add("-Dqpid.systest.logback.context=" + testClass.getName());
        if (System.getProperty("qpid.systest.remote_debugger") != null)
        {
            jvmArguments.add(System.getProperty("qpid.systest.remote_debugger"));
        }
        jvmArguments.add("org.apache.qpid.server.Main");
        jvmArguments.add("-prop");
        jvmArguments.add(String.format("qpid.work_dir=%s", escapePath(_currentWorkDirectory)));
        jvmArguments.add("--store-type");
        jvmArguments.add("JSON");
        jvmArguments.add("--initial-config-path");
        jvmArguments.add(escapePath(testInitialConfiguration.toString()));

        LOGGER.debug("Spawning broker JVM :", jvmArguments);

        String ready = System.getProperty(SYSTEST_PROPERTY_BROKER_READY, "BRK-1004 : Qpid Broker Ready");
        String stopped = System.getProperty(SYSTEST_PROPERTY_BROKER_STOPPED, "BRK-1005 : Stopped");
        String amqpListening = System.getProperty(SYSTEST_PROPERTY_BROKER_LISTENING,
                                                  "BRK-1002 : Starting( : \\w*)? : Listening on (\\w*) port ([0-9]+)");
        String process = System.getProperty(SYSTEST_PROPERTY_BROKER_PROCESS, "BRK-1017 : Process : PID : ([0-9]+)");
        int startUpTime = Integer.getInteger(SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME, 30000);

        LOGGER.debug("Spawning broker permitted start-up time: {}", startUpTime);

        String[] cmd = jvmArguments.toArray(new String[jvmArguments.size()]);

        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        processBuilder.redirectErrorStream(true);

        Map<String, String> processEnvironment = processBuilder.environment();
        processEnvironment.put("QPID_PNAME", "-DPNAME=QPBRKR -DTNAME=\"" + testClass.getName() + "\"");

        long startTime = System.currentTimeMillis();
        _process = processBuilder.start();

        BrokerSystemOutpuHandler brokerSystemOutpuHandler = new BrokerSystemOutpuHandler(_process.getInputStream(),
                                                                                         ready,
                                                                                         stopped,
                                                                                         process,
                                                                                         amqpListening,
                                                                                         getClass().getName());

        boolean brokerStarted = false;
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try
        {
            Future<?> result = executorService.submit(brokerSystemOutpuHandler);
            result.get(startUpTime, TimeUnit.MILLISECONDS);

            _pid = brokerSystemOutpuHandler.getPID();
            _ports = brokerSystemOutpuHandler.getAmqpPorts();

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
        catch (TimeoutException e)
        {
            LOGGER.warn("Spawned broker failed to become ready within {} ms. Ready line '{}'",
                        startUpTime, brokerSystemOutpuHandler.getReady());
            String threadDump = dumpThreads();
            if (!threadDump.isEmpty())
            {
                LOGGER.warn("the result of a try to capture thread dump:" + threadDump);
            }
            throw new BrokerAdminException(String.format("Broker failed to become ready within %d ms. Stop line : %s",
                                                         startUpTime,
                                                         brokerSystemOutpuHandler.getStopLine()));
        }
        catch (ExecutionException e)
        {
            throw new BrokerAdminException(String.format("Broker startup failed due to %s", e.getCause()),
                                           e.getCause());
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
            }
            executorService.shutdown();
        }
    }

    void createVirtualHost(final String virtualHostNodeName)
    {
        final String nodeType = System.getProperty(SYSTEST_PROPERTY_VIRTUALHOSTNODE_TYPE);
        _isPersistentStore = !"Memory".equals(nodeType);

        String storeDir = null;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else if (!"Memory".equals(nodeType))
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }

        String blueprint = System.getProperty(SYSTEST_PROPERTY_VIRTUALHOST_BLUEPRINT);
        LOGGER.debug("Creating Virtual host from blueprint: {}", blueprint);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", virtualHostNodeName);
        attributes.put("type", nodeType);
        attributes.put("qpid-type", nodeType);
        String contextAsString;
        try
        {
            contextAsString =
                    new ObjectMapper().writeValueAsString(Collections.singletonMap("virtualhostBlueprint", blueprint));
        }
        catch (JsonProcessingException e)
        {
            throw new BrokerAdminException("Cannot create virtual host as context serialization failed", e);
        }
        attributes.put("context", contextAsString);
        attributes.put("defaultVirtualHostNode", true);
        attributes.put("virtualHostInitialConfiguration", blueprint);
        if (storeDir != null)
        {
            attributes.put("storePath", storeDir);
        }

        try
        {
            Connection connection = createConnection("$management");
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    new AmqpManagementFacade().createEntityUsingAmqpManagement(virtualHostNodeName,
                                                                               "org.apache.qpid.VirtualHostNode",
                                                                               attributes,
                                                                               session);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    throw new BrokerAdminException(String.format("Cannot create test virtual host '%s'",
                                                                 virtualHostNodeName), e);
                }
                finally
                {
                    session.close();
                }
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot create virtual host '%s'", virtualHostNodeName), e);
        }
    }

    void deleteVirtualHost(final String virtualHostNodeName)
    {
        try
        {
            Connection connection = createConnection("$management");
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    new AmqpManagementFacade().deleteEntityUsingAmqpManagement(virtualHostNodeName,
                                                                               "org.apache.qpid.VirtualHostNode",
                                                                               session);
                }
                catch (AmqpManagementFacade.OperationUnsuccessfulException e)
                {
                    throw new BrokerAdminException(String.format("Cannot delete test virtual host '%s'",
                                                                 virtualHostNodeName), e);
                }
                finally
                {
                    session.close();
                }
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot delete virtual host '%s'", virtualHostNodeName), e);
        }
    }

    ListenableFuture<Void> restartVirtualHost(final String virtualHostNodeName)
    {
        try
        {
            Connection connection = createConnection("$management");
            try
            {
                connection.start();
                updateVirtualHostNode(virtualHostNodeName,
                                      Collections.<String, Object>singletonMap("desiredState", "STOPPED"), connection);
                updateVirtualHostNode(virtualHostNodeName,
                                      Collections.<String, Object>singletonMap("desiredState", "ACTIVE"), connection);
                return Futures.immediateFuture(null);
            }
            finally
            {
                connection.close();
            }
        }
        catch (JMSException e)
        {
            throw new BrokerAdminException(String.format("Cannot create virtual host %s", virtualHostNodeName), e);
        }
    }

    private void updateVirtualHostNode(final String virtualHostNodeName,
                                       final Map<String, Object> attributes,
                                       final Connection connection) throws JMSException
    {
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            new AmqpManagementFacade().updateEntityUsingAmqpManagement(virtualHostNodeName,
                                                                       "org.apache.qpid.VirtualHostNode",
                                                                       attributes,
                                                                       session);
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            throw new BrokerAdminException(String.format("Cannot create test virtual host '%s'", virtualHostNodeName),
                                           e);
        }
        finally
        {
            session.close();
        }
    }

    void shutdown()
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

    private String escapePath(String value)
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

    private Connection createConnection(String virtualHostName) throws JMSException
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY,
                                      "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        String urlTemplate = "amqp://:@%s/%s?brokerlist='tcp://localhost:%d?failover='nofailover''";
        String url = String.format(urlTemplate,
                                   "spawn_broker_admin",
                                   virtualHostName,
                                   getBrokerAddress(PortType.AMQP).getPort());
        initialContextEnvironment.put("connectionfactory." + factoryName, url);
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

    private void setClassQualifiedTestName(final String name)
    {
        final LoggerContext loggerContext = ((ch.qos.logback.classic.Logger) LOGGER).getLoggerContext();
        loggerContext.putProperty(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, name);
    }


    private String getVirtualHostNodeName(final Class testClass, final Method method)
    {
        return testClass.getSimpleName() + "_" + method.getName();
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

    private String dumpThreads()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            Process process = Runtime.getRuntime().exec("jstack " + _pid);
            InputStream is = process.getInputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1)
            {
                baos.write(buffer, 0, length);
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Error whilst collecting thread dump for " + _pid, e);
        }
        return new String(baos.toByteArray());
    }


    public final class BrokerSystemOutpuHandler implements Runnable
    {

        private final BufferedReader _in;
        private final Logger _out;
        private final String _ready;
        private final String _stopped;
        private final List<ListeningPort> _amqpPorts;
        private final Pattern _pidPattern;
        private final Pattern _amqpPortPattern;
        private volatile boolean _seenReady;
        private volatile String _stopLine;
        private volatile int _pid;

        private BrokerSystemOutpuHandler(InputStream in,
                                         String ready,
                                         String stopped,
                                         String pidRegExp,
                                         String amqpPortRegExp,
                                         String loggerName)
        {
            _amqpPorts = new ArrayList<>();
            _in = new BufferedReader(new InputStreamReader(in));
            _out = LoggerFactory.getLogger(loggerName);
            _ready = ready;
            _stopped = stopped;
            _seenReady = false;
            _amqpPortPattern = Pattern.compile(amqpPortRegExp);
            _pidPattern = Pattern.compile(pidRegExp);
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

                    if (line.contains(_ready))
                    {
                        _seenReady = true;
                        break;
                    }

                    if (!_seenReady && line.contains(_stopped))
                    {
                        _stopLine = line;
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
                                            portMatcher.group(2),
                                            Integer.parseInt(portMatcher.group(3))));
            }
        }

        String getStopLine()
        {
            return _stopLine;
        }

        String getReady()
        {
            return _ready;
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
        private String _protocol;
        private String _transport;
        private int _port;

        ListeningPort(final String protocol, final String transport, final int port)
        {
            _transport = transport;
            _port = port;
            _protocol = protocol;
        }

        String getTransport()
        {
            return _transport;
        }

        int getPort()
        {
            return _port;
        }

        String getProtocol()
        {
            return _protocol;
        }

        @Override
        public String toString()
        {
            return "ListeningPort{" +
                   "_protocol='" + _protocol + '\'' +
                   ", _transport='" + _transport + '\'' +
                   ", _port=" + _port +
                   '}';
        }
    }
}

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
package org.apache.qpid.systest.extension.encryption;


import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.systest.core.BrokerAdmin;
import org.apache.qpid.systest.core.JmsTestBase;
import org.apache.qpid.systest.core.brokerj.AmqpManagementFacade;

public class MessageEncryptionTest extends JmsTestBase
{
    private static final String PEER_STORE = "/tls/broker_peerstore.jks";
    private static final String KEY_STORE = "/tls/client_keystore.jks";
    private static final String STORE_PASSWORD = "password";

    private static final String TEST_MESSAGE_TEXT = "test message";
    private static final String ENCRYPTED_RECIPIENTS = "'CN=app1@acme.org, OU=art, O=acme, L=Toronto, ST=ON, C=CA'";
    private static final String QUEUE_ADDRESS_WITH_SEND_ENCRYPTED =
            "ADDR: %s ;  {x-send-encrypted : true, x-encrypted-recipients : " + ENCRYPTED_RECIPIENTS + "}";
    private static final String QUEUE_BURL_WITH_SEND_ENCRYPTED =
            "BURL:direct:///%s/%s?sendencrypted='true'&encryptedrecipients=" + ENCRYPTED_RECIPIENTS;

    private Path _trustStore;
    private Path _keyStore;
    private AmqpManagementFacade _managementFacade;

    @Before
    public void setUp() throws Exception
    {
        assumeThat("Strong encryption is not enabled",
                   isStrongEncryptionEnabled(),
                   is(equalTo(Boolean.TRUE)));

        assumeThat("Broker-j specific functionality is used by the test suite",
                   getBrokerAdmin().getBrokerType(),
                   is(equalTo(BrokerAdmin.BrokerType.BROKERJ)));

        _managementFacade = new AmqpManagementFacade();
        _trustStore = Files.createTempFile("trust_store", ".jks");
        _keyStore = Files.createTempFile("key_store", ".jks");

        try (final InputStream in = getClass().getResourceAsStream(KEY_STORE))
        {
            Files.copy(in, _keyStore, REPLACE_EXISTING);
        }

        try (final InputStream in = getClass().getResourceAsStream(PEER_STORE))
        {
            Files.copy(in, _trustStore, REPLACE_EXISTING);
        }
    }

    @After
    public void tearDown()
    {
        if (_trustStore != null)
        {
            _trustStore.toFile().delete();
            _trustStore = null;
        }

        if (_keyStore != null)
        {
            _keyStore.toFile().delete();
            _keyStore = null;
        }
    }

    @Test
    public void testEncryptionUsingMessageHeader() throws Exception
    {
        Connection producerConnection = getProducerConnection();
        try
        {
            Connection consumerConnection = getConsumerConnection();
            try
            {
                consumerConnection.start();
                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Queue queue = consumerSession.createQueue(getTestName());
                final MessageConsumer consumer = consumerSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = prodSession.createProducer(queue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                message.setBooleanProperty("x-qpid-encrypt", true);
                message.setStringProperty("x-qpid-encrypt-recipients",
                                          "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }


    @Test
    public void testEncryptionFromADDRAddress() throws Exception
    {
        String queueName = getTestName();
        Connection producerConnection = getProducerConnection();
        try
        {
            Connection consumerConnection = getConsumerConnection();
            try
            {
                consumerConnection.start();
                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Queue queue = consumerSession.createQueue(queueName);
                final MessageConsumer consumer = consumerSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue prodQueue = prodSession.createQueue(String.format(QUEUE_ADDRESS_WITH_SEND_ENCRYPTED, queueName));
                final MessageProducer producer = prodSession.createProducer(prodQueue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testEncryptionFromBURLAddress() throws Exception
    {

        String queueName = getTestName();
        Connection producerConnection = getProducerConnection();
        try
        {
            Connection consumerConnection = getConsumerConnection();
            try
            {
                consumerConnection.start();
                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Queue queue = consumerSession.createQueue(queueName);
                final MessageConsumer consumer = consumerSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue prodQueue =
                        prodSession.createQueue(String.format(QUEUE_BURL_WITH_SEND_ENCRYPTED, queueName, queueName));
                final MessageProducer producer = prodSession.createProducer(prodQueue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testBrokerAsTrustStoreProvider() throws Exception
    {
        String peerstore = "peerstore";
        addPeerStoreToBroker(peerstore, Collections.<String, Object>emptyMap());
        Connection producerConnection = getProducerConnectionWithRemoteTrustStore(peerstore);
        try
        {
            Connection consumerConnection = getConsumerConnection();
            try
            {
                consumerConnection.start();
                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Queue queue = consumerSession.createQueue(getTestQueueName());
                final MessageConsumer consumer = consumerSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = prodSession.createProducer(queue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                message.setBooleanProperty("x-qpid-encrypt", true);
                message.setStringProperty("x-qpid-encrypt-recipients",
                                          "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertTrue(receivedMessage instanceof TextMessage);
                assertEquals(TEST_MESSAGE_TEXT, ((TextMessage) message).getText());
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testBrokerStoreProviderWithExcludedVirtualHostNode() throws Exception
    {
        String testName = getTestName();

        String excludedVirtualHostNodeName = "vhn_" + testName;
        createTestVirtualHostNode(excludedVirtualHostNodeName);
        String peerstoreName = "peerstore_" + testName;
        addPeerStoreToBroker(peerstoreName,
                             Collections.<String, Object>singletonMap("excludedVirtualHostNodeMessageSources",
                                                                      "[\"" + excludedVirtualHostNodeName + "\"]"));

        Connection producerConnection =
                getProducerConnectionWithRemoteTrustStore(excludedVirtualHostNodeName, peerstoreName);
        try
        {

            final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = prodSession.createQueue(testName);
            prodSession.createConsumer(queue).close();

            final MessageProducer producer = prodSession.createProducer(queue);

            Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);
            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            try
            {
                producer.send(message);
                fail("Should not be able to send message");
            }
            catch (JMSException e)
            {
                assertTrue("Wrong exception cause: " + e.getCause(), e.getCause() instanceof CertificateException);
            }
        }
        finally
        {
            producerConnection.close();
        }
    }


    @Test
    public void testBrokerStoreProviderWithIncludedVirtualHostNode() throws Exception
    {
        String testName = getTestName();

        String includeVirtualHostNodeName = "vhn_" + testName;
        createTestVirtualHostNode(includeVirtualHostNodeName);

        String peerStoreName = "peerstore_" + testName;
        final Map<String, Object> additionalPeerStoreAttributes = new HashMap<>();
        String messageSources = "[\"" + includeVirtualHostNodeName + "\"]";
        additionalPeerStoreAttributes.put("includedVirtualHostNodeMessageSources", messageSources);
        // this is deliberate to test that the include list takes precedence
        additionalPeerStoreAttributes.put("excludedVirtualHostNodeMessageSources", messageSources);
        addPeerStoreToBroker(peerStoreName, additionalPeerStoreAttributes);

        Connection successfulProducerConnection =
                getProducerConnectionWithRemoteTrustStore(includeVirtualHostNodeName, peerStoreName);
        try
        {

            Connection failingProducerConnection = getProducerConnectionWithRemoteTrustStore(peerStoreName);

            final Session successfulSession =
                    successfulProducerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Queue queue = successfulSession.createQueue(testName);
            successfulSession.createConsumer(queue).close();

            final MessageProducer successfulProducer = successfulSession.createProducer(queue);
            final Session failingSession = failingProducerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer failingProducer = failingSession.createProducer(queue);

            Message message = successfulSession.createTextMessage(TEST_MESSAGE_TEXT);
            message.setBooleanProperty("x-qpid-encrypt", true);
            message.setStringProperty("x-qpid-encrypt-recipients",
                                      "cn=app1@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

            try
            {
                failingProducer.send(message);
                fail("Should not be able to send message");
            }
            catch (JMSException e)
            {
                assertTrue("Wrong exception cause: " + e.getCause(), e.getCause() instanceof CertificateException);
            }

            successfulProducer.send(message);
        }
        finally
        {
            successfulProducerConnection.close();
        }
    }

    @Test
    public void testUnknownRecipient() throws Exception
    {
        String peerstore = "peerstore_" + getTestName();
        addPeerStoreToBroker(peerstore, Collections.<String, Object>emptyMap());
        Connection producerConnection = getProducerConnectionWithRemoteTrustStore(peerstore);
        try
        {
            Connection consumerConnection = getConsumerConnection();
            try
            {
                consumerConnection.start();
                final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Queue queue = consumerSession.createQueue(getTestQueueName());
                final MessageConsumer consumer = consumerSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = prodSession.createProducer(queue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                message.setBooleanProperty("x-qpid-encrypt", true);
                message.setStringProperty("x-qpid-encrypt-recipients",
                                          "cn=unknwon@acme.org,ou=art,o=acme,l=toronto,st=on,c=ca");

                try
                {
                    producer.send(message);
                    fail("Should not have been able to send a message to an unknown recipient");
                }
                catch (JMSException e)
                {
                    // pass;
                }
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    @Test
    public void testRecipientHasNoValidCert() throws Exception
    {
        Connection producerConnection = getProducerConnection();
        try
        {
            Connection consumerConnection = getConnection();
            try
            {
                consumerConnection.start();
                final Session recvSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                String queueName = getTestName();
                Queue queue = recvSession.createQueue(queueName);
                final MessageConsumer consumer = recvSession.createConsumer(queue);

                final Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Queue prodQueue = prodSession.createQueue(String.format(QUEUE_ADDRESS_WITH_SEND_ENCRYPTED, queueName));

                final MessageProducer producer = prodSession.createProducer(prodQueue);

                Message message = prodSession.createTextMessage(TEST_MESSAGE_TEXT);

                producer.send(message);

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);
                assertFalse(receivedMessage instanceof TextMessage);
                assertTrue(receivedMessage instanceof BytesMessage);
            }
            finally
            {
                consumerConnection.close();
            }
        }
        finally
        {
            producerConnection.close();
        }
    }

    private void createTestVirtualHostNode(final String excludedVirtualHostNodeName) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("object-type", "JSON");
        attributes.put("type", "JSON");
        attributes.put("virtualHostInitialConfiguration",
                       String.format("{\"type\": \"%s\"}", "Memory"));

        createEntity(excludedVirtualHostNodeName, "org.apache.qpid.JsonVirtualHostNode", attributes);
    }

    private void addPeerStoreToBroker(final String peerStoreName,
                                      final Map<String, Object> additionalAttributes) throws Exception
    {
        Map<String, Object> peerStoreAttributes = new HashMap<>();
        peerStoreAttributes.put("name", peerStoreName);
        peerStoreAttributes.put("storeUrl", _trustStore.toFile().getAbsolutePath());
        peerStoreAttributes.put("password", STORE_PASSWORD);
        peerStoreAttributes.put("type", "FileTrustStore");
        peerStoreAttributes.put("qpid-type", "FileTrustStore");
        peerStoreAttributes.put("exposedAsMessageSource", true);
        peerStoreAttributes.putAll(additionalAttributes);

        createEntity(peerStoreName, "org.apache.qpid.server.security.FileTrustStore", peerStoreAttributes);
    }


    private void createEntity(final String entityName,
                              final String entityType,
                              final Map<String, Object> attributes) throws Exception
    {
        Connection connection = getBrokerManagementConnection();
        try
        {
            connection.start();
            _managementFacade.createEntityUsingAmqpManagement(entityName,
                                                              entityType,
                                                              attributes,
                                                              connection.createSession(false,
                                                                                       Session.AUTO_ACKNOWLEDGE));
        }
        finally
        {
            connection.close();
        }
    }


    private boolean isStrongEncryptionEnabled() throws NoSuchAlgorithmException
    {
        return Cipher.getMaxAllowedKeyLength("AES") >= 256;
    }

    private Connection getConsumerConnection() throws JMSException
    {
        final Map<String, String> receiverOptions = new HashMap<>();
        receiverOptions.put("encryption_key_store", _keyStore.toFile().getAbsolutePath());
        receiverOptions.put("encryption_key_store_password", STORE_PASSWORD);
        return getConnection(receiverOptions);
    }

    private Connection getProducerConnection() throws JMSException
    {
        final Map<String, String> producerOptions = new HashMap<>();
        producerOptions.put("encryption_trust_store", _trustStore.toFile().getAbsolutePath());
        producerOptions.put("encryption_trust_store_password", STORE_PASSWORD);
        return getConnection(producerOptions);
    }

    private Connection getProducerConnectionWithRemoteTrustStore(final String peerstore) throws JMSException
    {
        return getProducerConnectionWithRemoteTrustStore(null, peerstore);
    }


    private Connection getProducerConnectionWithRemoteTrustStore(final String virtualHostName,
                                                                 final String peerstoreName) throws JMSException
    {
        final Map<String, String> producerOptions = new HashMap<>();
        producerOptions.put("encryption_remote_trust_store", "$certificates%5c/" + peerstoreName);
        producerOptions.put("encryption_trust_store_password", STORE_PASSWORD);
        if (virtualHostName == null)
        {
            return getConnection(producerOptions);
        }
        return getBrokerAdmin().getConnection(virtualHostName, producerOptions);
    }
}

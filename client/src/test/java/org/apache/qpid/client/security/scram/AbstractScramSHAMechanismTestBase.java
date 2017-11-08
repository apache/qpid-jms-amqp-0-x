/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.client.security.scram;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;

import javax.security.sasl.SaslException;

import org.apache.qpid.test.utils.QpidTestCase;

/**
 * The quoted text in the test method javadoc is taken from RFC 5802.
 */
public abstract class AbstractScramSHAMechanismTestBase extends QpidTestCase
{
    private final byte[] expectedClientInitialResponse;
    private final byte[] serverFirstMessage;
    private final byte[] expectedClientFinalMessage;
    private final byte[] serverFinalMessage;

    public AbstractScramSHAMechanismTestBase(byte[] expectedClientInitialResponse,
                                             byte[] serverFirstMessage,
                                             byte[] expectedClientFinalMessage,
                                             byte[] serverFinalMessage)
    {
        this.expectedClientInitialResponse = expectedClientInitialResponse;
        this.serverFirstMessage = serverFirstMessage;
        this.expectedClientFinalMessage = expectedClientFinalMessage;
        this.serverFinalMessage = serverFinalMessage;
    }

    protected abstract AbstractScramSaslClient getScramSaslClient() throws Exception;
    protected abstract AbstractScramSaslClient getScramSaslClient(String username, String password) throws Exception;
    protected abstract String getExpectedInitialResponseString(final String escapedUsername);


    public void testSuccessfulAuthentication() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        byte[] clientInitialResponse = mechanism.evaluateChallenge(null);
        assertArrayEquals(expectedClientInitialResponse, clientInitialResponse);

        byte[] clientFinalMessage = mechanism.evaluateChallenge(serverFirstMessage);
        assertArrayEquals(expectedClientFinalMessage, clientFinalMessage);

        byte[] expectedFinalChallengeResponse = "".getBytes();
        assertArrayEquals(expectedFinalChallengeResponse, mechanism.evaluateChallenge(serverFinalMessage));

        assertTrue(mechanism.isComplete());
    }

    public void testServerFirstMessageMalformed() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        mechanism.evaluateChallenge(null);
        try
        {
            mechanism.evaluateChallenge("badserverfirst".getBytes());
            fail("Exception not thrown");
        }
        catch (SaslException s)
        {
            // PASS
        }
    }

    /**
     * 5.1.  SCRAM Attributes
     * "m: This attribute is reserved for future extensibility.  In this
     * version of SCRAM, its presence in a client or a server message
     * MUST cause authentication failure when the attribute is parsed by
     * the other end."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    public void testServerFirstMessageMandatoryExtensionRejected() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        mechanism.evaluateChallenge(null);
        try
        {
            mechanism.evaluateChallenge("m=notsupported,s=,i=".getBytes());
            fail("Exception not thrown");
        }
        catch (SaslException s)
        {
            // PASS
        }
    }

    /**
     * 5.  SCRAM Authentication Exchange
     * "In [the server first] response, the server sends a "server-first-message" containing the
     * user's iteration count i and the user's salt, and appends its own
     * nonce to the client-specified one."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    public void testServerFirstMessageInvalidNonceRejected() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        mechanism.evaluateChallenge(null);
        try
        {
            mechanism.evaluateChallenge("r=invalidnonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes());
            fail("Exception not thrown");
        }
        catch (SaslException s)
        {
            // PASS
        }
    }

    /**
     * 5.  SCRAM Authentication Exchange
     * "The client then authenticates the server by computing the
     * ServerSignature and comparing it to the value sent by the server.  If
     * the two are different, the client MUST consider the authentication
     * exchange to be unsuccessful, and it might have to drop the
     * connection."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    public void testServerSignatureDiffer() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        mechanism.evaluateChallenge(null);
        mechanism.evaluateChallenge(serverFirstMessage);
        try
        {
            mechanism.evaluateChallenge("v=badserverfinal".getBytes(StandardCharsets.US_ASCII));
            fail("Exception not thrown");
        }
        catch (SaslException e)
        {
            // PASS
        }
    }

    public void testIncompleteExchange() throws Exception
    {
        AbstractScramSaslClient mechanism = getScramSaslClient();

        byte[] clientInitialResponse = mechanism.evaluateChallenge(null);
        assertArrayEquals(expectedClientInitialResponse, clientInitialResponse);

        byte[] clientFinalMessage = mechanism.evaluateChallenge(serverFirstMessage);
        assertArrayEquals(expectedClientFinalMessage, clientFinalMessage);

        assertFalse(mechanism.isComplete());
    }

    public void testDifferentClientNonceOnEachInstance() throws Exception
    {
        AbstractScramSaslClient mech = getScramSaslClient();

        AbstractScramSaslClient mech2 = getScramSaslClient();

        byte[] clientInitialResponse = mech.evaluateChallenge(null);
        byte[] clientInitialResponse2 = mech2.evaluateChallenge(null);

        assertTrue(new String(clientInitialResponse, StandardCharsets.UTF_8).startsWith("n,,n=user,r="));
        assertTrue(new String(clientInitialResponse2, StandardCharsets.UTF_8).startsWith("n,,n=user,r="));

        assertNotEquals(clientInitialResponse, clientInitialResponse2);
    }


    public void testUsernameCommaEqualsCharactersEscaped() throws Exception
    {
        String originalUsername = "user,name=";
        String escapedUsername = "user=2Cname=3D";

        String expectedInitialResponseString = getExpectedInitialResponseString(escapedUsername);

        AbstractScramSaslClient mech = getScramSaslClient(originalUsername, "password");
        byte[] clientInitialResponse = mech.evaluateChallenge(null);
        assertArrayEquals(expectedInitialResponseString.getBytes(StandardCharsets.UTF_8), clientInitialResponse);
    }
}

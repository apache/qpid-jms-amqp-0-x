package org.apache.qpid.test.unit.message;

import static org.junit.Assert.assertArrayEquals;

import org.apache.qpid.client.message.AMQMessageDelegateFactory;
import org.apache.qpid.client.message.JMSTextMessage;
import org.apache.qpid.test.utils.QpidTestCase;

public class MessageTest extends QpidTestCase
{
    private JMSTextMessage _testTextMessage;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _testTextMessage = new JMSTextMessage(AMQMessageDelegateFactory.FACTORY_0_8);
        _testTextMessage.setText("testTextMessage text");
    }

    public void testJMSCorrelationIdAdBytesWithNonUtf8Bytes() throws Exception
    {
        // QPID-7897 and QPID-7899
        byte[] correlationId = new byte[]{(byte) 0xc3, 0x28};
        _testTextMessage.setJMSCorrelationIDAsBytes(correlationId);
        assertArrayEquals("", correlationId, _testTextMessage.getJMSCorrelationIDAsBytes());
    }
}

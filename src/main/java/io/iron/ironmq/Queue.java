package io.iron.ironmq;

import io.iron.ironmq.util.MessageBodyInflater;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Queue class represents a specific IronMQ queue bound to a client.
 */
public class Queue {
    Logger log = LoggerFactory.getLogger(getClass());
    final private Client client;
    final private String name;
    final private String baseUrl;

    public Queue(Client client, String name) {
        this.client = client;
        this.name = name;
        this.baseUrl = new StringBuilder().append("queues/").append(name)
                .append("/messages").toString();
    }

    /**
    * Retrieves a Message from the queue. If there are no items on the queue, an
    * EmptyQueueException is thrown.
    *
    * @throws EmptyQueueException If the queue is empty.
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public Message get() throws Exception {
        Messages msgs = get(1);
        Message msg;
        try {
            msg = msgs.getMessage(0);
        } catch (IndexOutOfBoundsException e) {
            throw new EmptyQueueException();
        }

        return msg;
    }

    /**
    * Retrieves Messages from the queue. If there are no items on the queue, an
    * EmptyQueueException is thrown.
    * @param numberOfMessages The number of messages to receive. Max. is 100.
    * @throws EmptyQueueException If the queue is empty.
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public Messages get(int numberOfMessages) throws Exception {
        Messages msgs = get(numberOfMessages, 120);
        if (msgs != null) {
            for (Message msg : msgs.getMessages()) {
                msg.setBody(MessageBodyInflater.inflateBody(msg.getBody()));
            }
        }
        return msgs;
    }

    /**
    * Retrieves Messages from the queue. If there are no items on the queue, an
    * EmptyQueueException is thrown.
    * @param numberOfMessages The number of messages to receive. Max. is 100.
    * @param timeout timeout in seconds.
    * @throws EmptyQueueException If the queue is empty.
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public Messages get(int numberOfMessages, int timeout) throws IOException {
        if (numberOfMessages < 0 || numberOfMessages > 100) {
            throw new IllegalArgumentException(
                    "numberOfMessages has to be within 1..100");
        }
        return client.get(new StringBuilder(baseUrl).append("?n=")
                .append(numberOfMessages).append("&timeout=").append(timeout)
                .toString());
    }

    /**
    * Deletes a Message from the queue.
    *
    * @param id The ID of the message to delete.
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public void deleteMessage(String id) throws IOException {
        client.delete(new StringBuilder(baseUrl).append("/").append(id)
                .toString());
    }

    /**
    * Deletes a Message from the queue.
    *
    * @param msg The message to delete.
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public void deleteMessage(Message msg) throws IOException {
        deleteMessage(msg.getId());
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg) throws Exception {
        return push(msg, null);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, Long expiresIn) throws Exception {
        return push(msg, expiresIn, null);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @param delay The message's delay in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, Long expiresIn, Long timeout)
            throws Exception {
        return push(msg, expiresIn, timeout, null);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @param delay The message's delay in seconds.
    * @param expiresIn The message's expiration offset in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, Long expiresIn, Long timeout, Long delay)
            throws Exception {
        Message message = new Message();
        byte[] msgBytes = msg.getBytes(Charset.forName("UTF-8"));
        log.debug("Original message length: {} bytes", msg.length());
        byte[] zippedBytes;
        InputStream bis = new ByteArrayInputStream(msgBytes);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DeflaterOutputStream dos = new DeflaterOutputStream(bos, new Deflater(
                Deflater.BEST_COMPRESSION));
        try {
            MessageBodyInflater.getBytes(bis, dos);
            dos.finish();
            bos.flush();
            zippedBytes = bos.toByteArray();
            message.setBody(Base64.encodeBase64URLSafeString(zippedBytes));
            log.debug("Compressed message length: {} bytes", message.getBody()
                    .length());
        } finally {
            if (bis != null)
                bis.close();
            bis = null;
            if (bos != null)
                bos.close();
            bos = null;
            if (dos != null)
                dos.close();
            dos = null;
        }
        message.setTimeout(timeout);
        message.setDelay(delay);
        message.setExpiresIn(expiresIn);

        Messages msgs = new Messages(message);
        // String body = client.getMapper().writeValueAsString(msgs);

        return client.post(baseUrl, msgs);
    }

    /**
     * Subscribe endpoints to a queue. This method will add unicast subscriber
     * @param subcrEndpoints
     * @return nothing. TO-DO return ironmq response for adding subscribers
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonGenerationException 
     */
    public void subscribers(String... subcrEndpoints)
            throws JsonGenerationException, JsonMappingException, IOException {
        subscribers(PushType.unicast, subcrEndpoints);
    }

    /**
     * Subscribe endpoints to a queue. This method will add unicast subscriber.
     * Currently no response is returned.
     * @param subcrEndpoints
     * @param pushType. unicast or multicast
     * @return nothing. TO-DO return ironmq response for adding subscribers 
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonGenerationException 
     */
    public void subscribers(PushType pushType, String... subcrEndpoints)
            throws JsonGenerationException, JsonMappingException, IOException {
        subscribers(pushType, Subscriber.RETRIES_COUNT, subcrEndpoints);
    }

    /**
     * Subscribe endpoints to a queue. This method will add unicast subscriber.
     * Currently no response is returned.
     * @param subcrEndpoints
     * @param pushType. unicast or multicast 
     * @param retries. Number of retries to push subscriber
     * @return nothing. TO-DO return ironmq response for adding subscribers
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonGenerationException 
     */
    public void subscribers(PushType pushType, int retries,
            String... subcrEndpoints) throws JsonGenerationException,
            JsonMappingException, IOException {
        subscribers(pushType, Subscriber.RETRIES_COUNT,
                Subscriber.RETRIES_DELAY, subcrEndpoints);
    }

    /**
     * Subscribe endpoints to a queue. This method will add unicast subscriber.
     * Currently no response is returned.
     * TO-DO return ironmq response for adding subscribers 
     * @param subcrEndpoints
     * @param pushType. unicast or multicast 
     * @param retries. Number of retries to push subscriber
     * @param retriesDelay. Delay between retries in seconds
     * @return
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonGenerationException 
     */
    public void subscribers(PushType pushType, int retries, int retriesDelay,
            String... subcrEndpoints) throws JsonGenerationException,
            JsonMappingException, IOException {
        assert (subcrEndpoints != null && subcrEndpoints.length > 0);

        Subscriber subscriber = new Subscriber();
        subscriber.pushType = pushType.name();
        subscriber.retries = retries;
        subscriber.retriesDelay = retriesDelay;
        for (final String endpoint : subcrEndpoints) {
            HashMap<String, String> endpointMap = new HashMap<String, String>();
            endpointMap.put(Subscriber.URL_KEY, endpoint);
            subscriber.endpoints.add(endpointMap);
        }
        client.post(new StringBuilder().append("queues/").append(name).append("/subscribers").toString(), subscriber);
    }

    /**
     * Clears the queue off all messages
     * @param queue the name of the queue 
     * @throws IOException
     */
    public void clear() throws IOException {
        client.post(new StringBuilder("queues/").append(name).append("/clear").toString(), "");
    }

    static class Info implements Serializable {
        private static final long serialVersionUID = 1L;
        int count;
        int size;
    }

}

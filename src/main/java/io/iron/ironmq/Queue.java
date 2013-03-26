package io.iron.ironmq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;

/**
 * The Queue class represents a specific IronMQ queue bound to a client.
 */
public class Queue {
    final private Client client;
    final private String name;

    public Queue(Client client, String name) {
        this.client = client;
        this.name = name;
    }

    /**
    * Retrieves a Message from the queue. If there are no items on the queue, an
    * EmptyQueueException is thrown.
    *
    * @throws EmptyQueueException If the queue is empty.
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public Message get() throws IOException {
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
    public Messages get(int numberOfMessages) throws IOException {
        return get(numberOfMessages, 120);
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
        Reader reader = client.get("queues/" + name + "/messages?n="
                + numberOfMessages + "&timeout=" + timeout);
        Messages messages = client.getMapper()
                .readValue(reader, Messages.class);
        reader.close();
        return messages;
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
        client.delete("queues/" + name + "/messages/" + id);
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
    public String push(String msg) throws IOException {
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
    public String push(String msg, Long timeout) throws IOException {
        return push(msg, timeout, null);
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
    public String push(String msg, Long timeout, Long delay) throws IOException {
        return push(msg, timeout, delay, null);
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
    public String push(String msg, Long timeout, Long delay, Long expiresIn)
            throws IOException {
        Message message = new Message();
        message.setBody(msg);
        message.setTimeout(timeout);
        message.setDelay(delay);
        message.setExpiresIn(expiresIn);

        Messages msgs = new Messages(message);
        String body = client.getMapper().writeValueAsString(msgs);

        Reader reader = client.post("queues/" + name + "/messages", body);
        BufferedReader bufReader = null;
        StringBuilder sb = new StringBuilder();
        try {
            bufReader = new BufferedReader(reader);
            while((msg=bufReader.readLine()) != null)
                sb.append(msg);
        } catch (JsonMappingException e) {
            msg = "IronMQ's response contained invalid JSON";
        } finally {
            if (reader != null)
                reader.close();
        }
        return sb.toString();
    }

    /**
     * Subscribe endpoints to a queue. This method will add unicast subscriber
     * @param subcrEndpoints
     * @return nothing. TO-DO return ironmq response for adding subscribers
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonGenerationException 
     */
    public void subscribers(String... subcrEndpoints) throws JsonGenerationException, JsonMappingException, IOException {
        subscribers(PushType.unicast,subcrEndpoints);
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
    public void subscribers(PushType pushType, String... subcrEndpoints) throws JsonGenerationException, JsonMappingException, IOException {
        subscribers(pushType,Subscriber.RETRIES_COUNT,subcrEndpoints);
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
    public void subscribers(PushType pushType, int retries, String... subcrEndpoints) throws JsonGenerationException, JsonMappingException, IOException {
        subscribers(pushType,Subscriber.RETRIES_COUNT,Subscriber.RETRIES_DELAY,subcrEndpoints);
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
    public void subscribers(PushType pushType, int retries,int retriesDelay, String... subcrEndpoints) throws JsonGenerationException, JsonMappingException, IOException {
        assert(subcrEndpoints != null && subcrEndpoints.length >0);
        
        Subscriber subscriber = new Subscriber();
        subscriber.pushType = pushType.name();
        subscriber.retries = retries;
        subscriber.retriesDelay = retriesDelay;
        for (final String endpoint : subcrEndpoints) 
            subscriber.endpoints.add(new HashMap<String, String>(){{
                put(Subscriber.URL_KEY, endpoint);}});
        client.post("queues/"+name+"/subscribers", client.getMapper().writeValueAsString(subscriber)).close();
    }

    /**
     * Clears the queue off all messages
     * @param queue the name of the queue 
     * @throws IOException
     */
    public void clear() throws IOException {
        client.post("queues/" + name + "/clear", "").close();
    }

    public int getSize() throws IOException {
        Reader reader = client.get("queues/" + name);
        Info info = client.getMapper().readValue(reader, Info.class);
        reader.close();
        return info.size;
    }

    static class Info implements Serializable {
        int count;
        int size;
    }
    
    static class Subscriber implements Serializable {
        static final int RETRIES_COUNT = 3;
        static final int RETRIES_DELAY = 60;
        static final String URL_KEY = "url";
        int retries;
        @JsonProperty("retries_delay") int retriesDelay;
        @JsonProperty("push_type") String pushType;
        @JsonProperty("subscribers") List<HashMap<String,String>> endpoints = new ArrayList<HashMap<String,String>>();

        public Subscriber() {
        }
        
    }
}

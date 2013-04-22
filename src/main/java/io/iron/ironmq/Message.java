package io.iron.ironmq;

import java.io.Serializable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonRawValue;

/**
 * The Message class represents a message retrieved from an IronMQ queue.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    // @JsonRawValue
    private String body;
    private Long timeout;
    private Long delay;
    // Long, not long, so that it's nullable. Gson doesn't serialize null,
    // so we can use the default on the server and not have to know about
    // it.
    @JsonProperty("expires_in")
    @JsonRawValue
    private Long expiresIn;

    @JsonIgnore
    private Object push_status;

    public Message() {
    }

    /**
    * Returns the Message's body contents.
    */
    public String getBody() {
        return body;
    }

    /**
    * Sets the Message's body contents.
    *
    * @param body The new body contents.
    */
    public void setBody(String body) {
        this.body = body;
    }

    /**
    * Returns the Message's ID.
    */
    public String getId() {
        return id;
    }

    /**
    * Sets the Message's ID.
    *
    * @param id The new ID.
    */
    public void setId(String id) {
        this.id = id;
    }

    /**
    * Returns the Message's timeout.
    */
    public Long getTimeout() {
        return timeout;
    }

    /**
    * Sets the Message's timeout.
    *
    * @param timeout The new timeout.
    */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    /**
    * Returns the number of seconds after which the Message will be available.
    */
    public Long getDelay() {
        return delay;
    }

    /**
    * Sets the number of seconds after which the Message will be available.
    *
    * @param delay The new delay.
    */
    public void setDelay(Long delay) {
        this.delay = delay;
    }

    /**
    * Returns the number of seconds in which the Message will be removed from the
    * queue. If the server default of 7 days will be used, 0 is returned.
    */
    public Long getExpiresIn() {
        return this.expiresIn;
    }

    /**
    * Sets the number of seconds in which the Message will be removed from the
    * queue.
    *
    * @param expiresIn The new expiration offset in seconds. A value less than
    * or equal to 0 will cause the server default of 7 days to be used.
    */
    public void setExpiresIn(Long expiresIn) {
        this.expiresIn = expiresIn;
    }

    /**
    * Returns a string representation of the Message.
    */
    public String toString() {
        return body;
    }

    /**
     * @return the push_status
     */
    public Object getPush_status() {
        return push_status;
    }

    /**
     * @param push_status the push_status to set
     */
    public void setPush_status(Object push_status) {
        this.push_status = push_status;
    }

}

package io.iron.ironmq;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Messages {
    private Message[] messages;

    public Messages() {}
    public Messages(Message... msgs) {
        messages = msgs;
    }

    public Message getMessage(int i) {
        return messages[i];
    }

    public Message[] getMessages() {
        return messages;
    }
}

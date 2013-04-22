package io.iron.ironmq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;

public class Subscriber implements Serializable {
    private static final long serialVersionUID = 1L;
    static final int RETRIES_COUNT = 3;
    static final int RETRIES_DELAY = 60;
    static final String URL_KEY = "url";
    int retries;
    @JsonProperty("retries_delay")
    int retriesDelay;
    @JsonProperty("push_type")
    String pushType;
    @JsonProperty("subscribers")
    List<HashMap<String, String>> endpoints = new ArrayList<HashMap<String, String>>();

    public Subscriber() {
    }

}

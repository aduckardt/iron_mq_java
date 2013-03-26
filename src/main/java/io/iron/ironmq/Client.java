package io.iron.ironmq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * The Client class provides access to the IronMQ service.
 */
public class Client {
    static final private String apiVersion = "1";

    static final Random rand = new Random();

    private String projectId;
    private String token;
    private Cloud cloud;
    private ObjectMapper mapper;

    static {
        System.setProperty("https.protocols", "TLSv1");
    }

    /**
     * Constructs a new Client using the specified project ID and token.
     * The network is not accessed during construction and this call will
     * succeed even if the credentials are invalid.
     * This constructor uses the AWS cloud with the US East region.
     *
     * @param projectId A 24-character project ID.
     * @param token An OAuth token.
     * @param Jackson object mapper for data binding
     */
    public Client(String projectId, String token, ObjectMapper mapper) {
        this(projectId, token, Cloud.ironAWSUSEast, mapper);
    }

    /**
     * Constructs a new Client using the specified project ID and token.
     * The network is not accessed during construction and this call will
     * succeed even if the credentials are invalid.
     *
     * @param projectId A 24-character project ID.
     * @param token An OAuth token.
     * @param cloud The cloud to use.
     * @param Jackson object mapper for data binding
     */
    public Client(String projectId, String token, Cloud cloud, ObjectMapper mapper) {
        this.projectId = projectId;
        this.token = token;
        this.cloud = cloud;
        this.mapper = mapper;
    }

    /**
     * Returns a Queue using the given name.
     * The network is not accessed during this call.
     *
     * @param name The name of the Queue to create.
     */
    public Queue queue(String name) {
        return new Queue(this, name);
    }

    Reader delete(String endpoint) throws IOException {
        return request("DELETE", endpoint, null);
    }

    Reader get(String endpoint) throws IOException {
        return request("GET", endpoint, null);
    }

    Reader post(String endpoint, String body) throws IOException {
        return request("POST", endpoint, body);
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    private Reader request(String method, String endpoint, String body) throws IOException {
        String path = "/" + apiVersion + "/projects/" + projectId + "/" + endpoint;
        URL url = new URL(cloud.scheme, cloud.host, cloud.port, path);

        final int maxRetries = 5;
        int retries = 0;
        while (true) {
            try {
                return singleRequest(method, url, body);
            } catch (HTTPException e) {
                // ELB sometimes returns this when load is increasing.
                // We retry with exponential backoff.
                if (e.getStatusCode() != 503 || retries >= maxRetries) {
                    throw e;
                }
                retries++;
                // random delay between 0 and 4^tries*100 milliseconds
                int pow = (1 << (2*retries))*100;
                int delay = rand.nextInt(pow);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    static private class Error implements Serializable {
        String msg;
    }

    private Reader singleRequest(String method, URL url, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "OAuth " + token);
        conn.setRequestProperty("User-Agent", "IronMQ Java Client");

        if (body != null) {
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
        }

        conn.connect();

        if (body != null) {
            OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
            out.write(body);
            out.close();
        }

        int status = conn.getResponseCode();
        if (status != 200) {
            StringBuilder sb=  new StringBuilder();
            String msg;
            if (conn.getContentLength() > 0 && conn.getContentType().equals("application/json")) {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                    while((msg=reader.readLine()) != null)
                        sb.append(msg);
//                    Error error = mapper.readValue(reader, Error.class);
                    msg = sb.toString();
                } catch (JsonMappingException e) {
                    msg = "IronMQ's response contained invalid JSON";
                } finally {
                    if (reader != null)
                        reader.close();
                }
            } else {
                msg = "Empty or non-JSON response";
            }
            throw new HTTPException(status, msg);
        }

        return new InputStreamReader(conn.getInputStream());
    }
}


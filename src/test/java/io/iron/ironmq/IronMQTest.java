package io.iron.ironmq;

import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class IronMQTest {
    private String projectId;
    private String token;

    @Before
    public void setup() {
        projectId = System.getenv("IRON_PROJECT_ID");
        token = System.getenv("IRON_TOKEN");
        Assume.assumeTrue(projectId != null && token != null);
    }

    @Test
    public void testClient() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // mapper.configure(Feature.WRITE_NUMBERS_AS_STRINGS, false);
        mapper.setSerializationInclusion(Inclusion.NON_EMPTY);
        mapper.setVisibilityChecker(mapper.getVisibilityChecker()
                .withCreatorVisibility(Visibility.NONE)
                .withGetterVisibility(Visibility.NONE)
                .withIsGetterVisibility(Visibility.NONE)
                .withFieldVisibility(Visibility.ANY));
        Client c = new Client(projectId, token, Cloud.ironAWSUSEast, mapper);
        Queue q = c.queue("test-queue");

        // q.clear();

        // Assert.assertEquals(0, q.getSize());

        final String body = "Hello World ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ->.1234567890!";
        q.push(body, 30l);

        // Assert.assertEquals(1, q.getSize());\
        // try {
        // Thread.sleep(20000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        Message msg = q.get();
        Assert.assertEquals(body, msg.getBody());
        // Assert.assertEquals(id, msg.getId());
        q.deleteMessage(msg);
    }

    @Test(expected = HTTPException.class)
    public void testErrorResponse() throws Exception {
        // intentionally invalid project/token combination
        Client c = new Client("4444444444444", "aaaaaa", Cloud.ironAWSUSEast,
                new ObjectMapper());
        Queue q = c.queue("test-queue");

        q.push("123456789012345678901234567890");
    }
}

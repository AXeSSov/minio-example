import com.google.common.collect.Lists;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.*;
import org.junit.Test;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class TestMinio {

    public static final String TESTUPLOAD = "testupload";

    @Test
    public void upload() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        MinioClient minioClient = MinioClient.builder().endpoint("http://localhost:9000").credentials("minioadmin", "minioadmin").build();
        String bucketName = TESTUPLOAD;
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "tagvalue1");
        tags.put("tag2", "tagvalue2");

        String objectName = UUID.randomUUID().toString();
        String content = "Testing upload " + objectName;
        minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).tags(tags).contentType("text")
                .object("test/" + objectName).stream(new ByteArrayInputStream(content.getBytes()), -1, 10485760).build());
    }

    @Test
    public void listBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        MinioClient minioClient = MinioClient.builder().endpoint("http://localhost:9000").credentials("minioadmin", "minioadmin").build();
        List<Bucket> buckets = minioClient.listBuckets();
        for (Bucket bucket : buckets) {
            System.err.println(bucket.name());
        }
    }

    @Test
    public void listObjects() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        MinioClient minioClient = MinioClient.builder().endpoint("http://localhost:9000").credentials("minioadmin", "minioadmin").build();
        for (Result<Item> itemResult : minioClient.listObjects(ListObjectsArgs.builder().prefix("test/").bucket(TESTUPLOAD).build())) {
            System.err.println(itemResult.get().objectName());
        }
    }

    @Test
    public void getObject() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        MinioClient minioClient = MinioClient.builder().endpoint("http://localhost:9000").credentials("minioadmin", "minioadmin").build();
        for (Result<Item> itemResult : minioClient.listObjects(ListObjectsArgs.builder().bucket(TESTUPLOAD).build())) {
            String objectName = itemResult.get().objectName();
            System.err.println("Object name: " + objectName + (itemResult.get().isDir() ? " is dir" : " is file"));

            if (!itemResult.get().isDir()) {
                System.err.println("Content:");
                //Getting data
                InputStream stream = minioClient.getObject(GetObjectArgs.builder().bucket(TESTUPLOAD).object(objectName).build());

                System.err.println(new BufferedReader(new InputStreamReader(stream))
                        .lines().collect(Collectors.joining("\n")));
                stream.close();

                //Getting tags
                Tags objectTags = minioClient.getObjectTags(GetObjectTagsArgs.builder().bucket(TESTUPLOAD).object(objectName).build());

                System.err.println("Tags: \n" + objectTags.get().entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining("\n")));
            }
            System.err.println("----------------------------");
        }
    }

    //Set expiration date for bucket - by tag
    @Test
    public void setBucketLifeCycle() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        MinioClient minioClient = MinioClient.builder().endpoint("http://localhost:9000").credentials("minioadmin", "minioadmin").build();
        LifecycleRule rule = new LifecycleRule(Status.ENABLED, null, new Expiration((ZonedDateTime) null, 1, null), new RuleFilter(new Tag("tag1", "tagvalue1")),
                "expireByTag1", null, null, null);
        LifecycleConfiguration configuration = new LifecycleConfiguration(Arrays.asList(rule));
        minioClient.setBucketLifecycle(SetBucketLifecycleArgs.builder().bucket(TESTUPLOAD).config(configuration).build());
    }

}

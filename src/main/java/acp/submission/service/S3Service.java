package acp.submission.service;

import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
public class S3Service {

    private final S3Client s3Client;

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Read a single object from S3 and return its contents as a String
     */
    public String readOne(String bucket, String key) {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        ResponseBytes<GetObjectResponse> bytes =
                s3Client.getObject(req, ResponseTransformer.toBytes());

        return bytes.asString(StandardCharsets.UTF_8);
    }

    public List<String> readAll(String bucket) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);

        List<String> contents = new ArrayList<>();

        for (S3Object obj : response.contents()) {
            String key = obj.key();
            if (key == null || key.endsWith("/")) continue;
            contents.add(readOne(bucket, key));
        }

        return contents;
    }
}

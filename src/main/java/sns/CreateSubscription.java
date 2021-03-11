package sns;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;

public class CreateSubscription {

    @Value("${aws.access-key}")
    private static String accessKey;

    @Value("${aws.secret-key}")
    private static String secretKey;

    @Value("${aws.sns-resource}")
    private static String topicArn;

    public static void main(String args[]) throws IOException {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonSNS snsClient = AmazonSNSClient.builder().withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();

        final SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, "email", "edevarfilho@gmail.com");

        snsClient.subscribe(subscribeRequest);
        System.out.println(snsClient.getCachedResponseMetadata(subscribeRequest));
    }

}


package sns;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.*;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.sns.AmazonSNSExtendedClient;
import software.amazon.sns.SNSExtendedClientConfiguration;

import java.io.IOException;

public class IntegrationAWSComponentsPublishing {

    @Value("${aws.access-key}")
    private static String accessKey;

    @Value("${aws.secret-key}")
    private static String secretKey;

    @Value("${aws.sns-resource}")
    private static String topicArn;

    public static void main(String args[]) throws IOException {
        final String BUCKET_NAME = "bucket-payload-springboot";
        final String TOPIC_NAME = "topic-payload-springboot";
        final String QUEUE_NAME = "queue-payload-springboot";

        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        AmazonSNS snsClient = AmazonSNSClient.builder().withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();

        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();

        s3Client.createBucket(BUCKET_NAME);

        final String topicArn = snsClient.createTopic(new CreateTopicRequest().withName(TOPIC_NAME)).getTopicArn();

        final String queueUrl =  sqsClient.createQueue(new CreateQueueRequest().withQueueName(QUEUE_NAME)).getQueueUrl();

        final String subscriptionArn = Topics.subscribeQueue(snsClient, sqsClient, topicArn, queueUrl);

        /*final SetSubscriptionAttributesRequest setSubscriptionAttributesRequest = new SetSubscriptionAttributesRequest();
        setSubscriptionAttributesRequest.setSubscriptionArn(subscriptionArn);
        setSubscriptionAttributesRequest.setAttributeName("sprinbootMessageDelivery");
        setSubscriptionAttributesRequest.setAttributeValue("TRUE");

        snsClient.setSubscriptionAttributes(setSubscriptionAttributesRequest);*/

        SNSExtendedClientConfiguration snsExtendedClientConfiguration = new SNSExtendedClientConfiguration()
                .withPayloadSupportEnabled(s3Client, BUCKET_NAME)
                .withPayloadSizeThreshold(32);

        final AmazonSNSExtendedClient amazonSNSExtendedClient = new AmazonSNSExtendedClient(snsClient, snsExtendedClientConfiguration);
        final String message = "Any message greater than 32 bytes, store in S3";

        final PublishResult publishResult = amazonSNSExtendedClient.publish(topicArn, message);

    }

}


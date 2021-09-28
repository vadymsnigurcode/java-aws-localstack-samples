import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;

public class TestAwsS3 {

    // CLI:
    // aws s3 mb s3://bucket-name
    @Test
    public void testCreateS3BucketUsingCredentials() {
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "my-bucket";

        try {
            //AWSCredentials awsCredentials = new BasicAWSCredentials("test","test");
            //AWSCredentials awsCredentials = new BasicAWSCredentials("test","test");

            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            //builder.withCredentials(new AWSStaticCredentialsProvider(awsCredentials));
            builder.withEndpointConfiguration(new AwsClientBuilder
                    .EndpointConfiguration("http://s3.localhost.localstack.cloud:4566", "us-west-2"));
            AmazonS3 s3Client = builder.build();

            if (!s3Client.doesBucketExistV2(bucketName)) {
                // Because the CreateBucketRequest object doesn't specify a region, the
                // bucket is created in the region specified in the client.
                s3Client.createBucket(new CreateBucketRequest(bucketName));

                // Verify that the bucket was created by retrieving it and checking its location.
                String bucketLocation = s3Client.getBucketLocation(new GetBucketLocationRequest(bucketName));
                System.out.println("Bucket location: " + bucketLocation);
            }
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it and returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    // CLI:
    // aws s3 mb s3://bucket-name
    @Test
    public void testCreateS3Bucket() {
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "my-bucket";

        try {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            builder.withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                    .setPathStyleAccessEnabled(true);

            builder.withPathStyleAccessEnabled(true);
            AmazonS3 s3Client = builder.build();

            if (!s3Client.doesBucketExistV2(bucketName)) {
                // Because the CreateBucketRequest object doesn't specify a region, the
                // bucket is created in the region specified in the client.
                s3Client.createBucket(new CreateBucketRequest(bucketName));

                // Verify that the bucket was created by retrieving it and checking its location.
                String bucketLocation = s3Client.getBucketLocation(new GetBucketLocationRequest(bucketName));
                System.out.println("Bucket location: " + bucketLocation);
            }
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it and returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    //cli:
    // aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket my-bucket --region us-east-1
    @Test
    public void testCreateS3Object() {
        String bucket_name = "my-bucket";
        String file_path = "D:\\!Java_corse\\the ticket master ex\\dynamo-db\\src\\main\\resources\\data.json";
        //String region = "us-west-2";
        String key_name = "data.json";
        System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket_name);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                .setPathStyleAccessEnabled(true);

        builder.withPathStyleAccessEnabled(true);
        AmazonS3 s3 = builder.build();
        try {
            s3.putObject(bucket_name, key_name, new File(file_path));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

    }
    @Test
    public void testS3ListObjects() {
        String bucket_name = "my-bucket";
        System.out.format("Objects in S3 bucket %s:\n", bucket_name);

        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                .setPathStyleAccessEnabled(true);

        builder.withPathStyleAccessEnabled(true);
        AmazonS3 s3Client = builder.build();


        ListObjectsV2Result result = s3Client.listObjectsV2(bucket_name);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            System.out.println("* " + os.getKey());
        }
    }

    //cli:
    // aws --endpoint-url=http://localhost:4566 s3 rb s3://my-bucket --force
    // aws --endpoint-url=http://localhost:4566 s3 rm s3://bucket-name --recursive
    @Test
    public void testRemoveS3Bucket() {

        String bucketName = "my-bucket";

        try {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            builder.withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                    .setPathStyleAccessEnabled(true);

            builder.withPathStyleAccessEnabled(true);
            AmazonS3 s3Client = builder.build();

            // Delete all objects from the bucket. This is sufficient
            // for unversioned buckets. For versioned buckets, when you attempt to delete objects, Amazon S3 inserts
            // delete markers for all objects, but doesn't delete the object versions.
            // To delete objects from versioned buckets, delete all of the object versions before deleting
            // the bucket (see below for an example).
            ObjectListing objectListing = s3Client.listObjects(bucketName);
            while (true) {
                Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
                while (objIter.hasNext()) {
                    s3Client.deleteObject(bucketName, objIter.next().getKey());
                }

                // If the bucket contains many objects, the listObjects() call
                // might not return all of the objects in the first listing. Check to
                // see whether the listing was truncated. If so, retrieve the next page of objects
                // and delete them.
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            // Delete all object versions (required for versioned buckets).
            VersionListing versionList = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
                while (versionIter.hasNext()) {
                    S3VersionSummary vs = versionIter.next();
                    s3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }

                if (versionList.isTruncated()) {
                    versionList = s3Client.listNextBatchOfVersions(versionList);
                } else {
                    break;
                }
            }

            // After all objects and object versions are deleted, delete the bucket.
            s3Client.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client couldn't
            // parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    @Test
    public void testListS3Objects() {

        String bucketName = "my-bucket";

        try {
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
            builder.withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                    .setPathStyleAccessEnabled(true);

            builder.withPathStyleAccessEnabled(true);
            AmazonS3 s3Client = builder.build();

            System.out.println("Listing objects");

            // maxKeys is set to 2 to demonstrate the use of
            // ListObjectsV2Result.getNextContinuationToken()
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
            ListObjectsV2Result result;

            do {
                result = s3Client.listObjectsV2(req);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    System.out.printf(" - %s (size: %d)\n", objectSummary.getKey(), objectSummary.getSize());
                }
                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                String token = result.getNextContinuationToken();
                System.out.println("Next Continuation Token: " + token);
                req.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }
}

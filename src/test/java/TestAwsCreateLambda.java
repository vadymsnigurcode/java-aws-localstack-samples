import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.*;
import org.junit.Test;

public class TestAwsCreateLambda {
    // CLI:
    // aws --endpoint-url=http://localhost:4566 lambda list-functions
    @Test
    public void createLocaltackLambda() {
         /*
        Function names appear as arn:aws:lambda:us-west-2:335556330391:function:HelloFunction
        you can retrieve the value by looking at the function in the AWS Console
         */

        // snippet-start:[lambda.java1.invoke.main]
        String functionName = "simple-lambda-01";

        InvokeRequest invokeRequest = new InvokeRequest()
                .withFunctionName(functionName)
                .withPayload("{\n" +
                        " \"Hello \": \"Paris\",\n" +
                        " \"countryCode\": \"FR\"\n" +
                        "}");
        InvokeResult invokeResult = null;

        try {
            AWSLambda awsLambda = AWSLambdaClientBuilder.standard()
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                    //.withRegion(Regions.US_WEST_2)
                    .build();

            CreateFunctionRequest createFunctionRequest = new CreateFunctionRequest();
            createFunctionRequest.setFunctionName("testLambda");
            CreateFunctionResult createResult = awsLambda.createFunction(new CreateFunctionRequest());
//            invokeResult = awsLambda.invoke(invokeRequest);
//
//            String ans = new String(invokeResult.getPayload().array(), StandardCharsets.UTF_8);
//
//            //write out the return value
//            System.out.println(ans);

        } catch (ServiceException e) {
            System.out.println(e);
        }

        System.out.println(invokeResult.getStatusCode());
        // snippet-end:[lambda.java1.invoke.main]
    }
}

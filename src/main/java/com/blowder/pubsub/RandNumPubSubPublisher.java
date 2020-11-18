package com.blowder.pubsub;

import com.blowder.model.NumberWrapperOuterClass;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandNumPubSubPublisher {
    public static void main(String[] args) throws IOException, InterruptedException {
        TopicName topicName = TopicName.of(
                "boxwood-sector-246122",
                "flow_rand_numbers_test"
        );
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();

            while (true) {
                NumberWrapperOuterClass.NumberWrapper number = NumberWrapperOuterClass.NumberWrapper.newBuilder()
                        .setNumber(new Random().nextInt(100)).build();
                ApiFuture<String> publish = publisher.publish(PubsubMessage.newBuilder().setData(number.toByteString()).build());
                ApiFutures.addCallback(publish,
                        new ApiFutureCallback<String>() {
                            @Override
                            public void onFailure(Throwable throwable) {
                                System.err.println(throwable.getMessage());
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                System.out.printf("Message with id='%s' was published\n", messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
                Thread.sleep(100);
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}

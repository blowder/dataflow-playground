package com.blowder.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

public class GooglePubSubConsumer {
    public static void main(String[] args) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of("boxwood-sector-246122", "first_topic_rand_numbers_subscription");
        MessageReceiver receiver = (message, consumer) -> {
            System.out.printf("Message '%s' is consumed\n", message.getData().toStringUtf8());
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            subscriber.awaitTerminated(); //wait forever
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }
}

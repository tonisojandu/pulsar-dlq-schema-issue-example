package dlq.issue

import java.util.concurrent.TimeUnit
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.DeadLetterPolicy
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.shaded.org.awaitility.Awaitility
import org.testcontainers.utility.DockerImageName

val TOPIC = "test-topic"
val DLQ_TOPIC = "dlq-topic"
val RETRY_TOPIC = "retry-topic"

fun main() {
  PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.1.0")).use { pulsar ->
    pulsar.start()

    val admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.httpServiceUrl).build()
    val client = PulsarClient.builder().serviceUrl(pulsar.pulsarBrokerUrl).build()

    client
        .newConsumer(Schema.AVRO(SomeRecord::class.java))
        .topic(TOPIC)
        .subscriptionType(SubscriptionType.Shared)
        .subscriptionName("test-subscription")
        .enableRetry(false)
        .deadLetterPolicy(
            DeadLetterPolicy.builder()
                .deadLetterTopic(DLQ_TOPIC)
                .retryLetterTopic(RETRY_TOPIC)
                .maxRedeliverCount(1)
                .build())
        .ackTimeoutTickTime(500, TimeUnit.MILLISECONDS)
        .acknowledgmentGroupTime(10, TimeUnit.MILLISECONDS)
        .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
        .messageListener { consumer, msg -> consumer.negativeAcknowledge(msg) }
        .subscribe()

    removeSchemaValidationPolicies(admin)

    val producer =
        client.newProducer(Schema.AVRO(IncompatibleRecord::class.java)).topic(TOPIC).create()
    producer.send(IncompatibleRecord("test"))

    keepTestContainerAlive()
  }
}

private fun removeSchemaValidationPolicies(admin: PulsarAdmin) {
  admin.namespaces().setSchemaValidationEnforced("public/default", false)
  admin
      .topicPolicies()
      .setSchemaCompatibilityStrategy(TOPIC, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE)

  Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
    !admin.namespaces().getSchemaValidationEnforced("public/default") &&
        admin.topicPolicies().getSchemaCompatibilityStrategy(TOPIC, true) ==
            SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE
  }
}

private fun keepTestContainerAlive() {
  while (!Thread.currentThread().isInterrupted) {
    try {
      Thread.sleep(1000)
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
    }
  }
}

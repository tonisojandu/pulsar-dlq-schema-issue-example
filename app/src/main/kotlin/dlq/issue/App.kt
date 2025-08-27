package dlq.issue

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.DeadLetterPolicy
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy
import org.apache.pulsar.common.policies.data.TenantInfo
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.shaded.org.awaitility.Awaitility.*
import org.testcontainers.utility.DockerImageName

const val TENANT = "platform"
const val TENANT_NAMESPACE = "$TENANT/test"
const val TOPIC = "$TENANT_NAMESPACE/test-topic"
const val DLQ_TOPIC = "$TENANT_NAMESPACE/dlq-topic"
const val RETRY_TOPIC = "$TENANT_NAMESPACE/retry-topic"

private val log = LoggerFactory.getLogger("main")

fun main() {
  var resultMsg = "\n\n\tFailed to read message from DLQ\n"

  try {
    PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.6")).use { pulsar ->
      pulsar.start()

      val admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.httpServiceUrl).build()
      val client = PulsarClient.builder().serviceUrl(pulsar.pulsarBrokerUrl).build()

      provision(admin)

      val moveForward = AtomicBoolean(false)

      val consumer =
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
              .build()
          )
          .ackTimeoutTickTime(500, TimeUnit.MILLISECONDS)
          .acknowledgmentGroupTime(10, TimeUnit.MILLISECONDS)
          .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS)
          .messageListener { consumer, msg ->
            moveForward.set(true)
            consumer.negativeAcknowledge(msg)
          }
          .subscribe()

      val producer =
        client.newProducer(Schema.AVRO(IncompatibleRecord::class.java)).topic(TOPIC).create()
      producer.send(IncompatibleRecord("test"))

      await().atMost(10, TimeUnit.SECONDS).untilTrue(moveForward)

      // Let's ensure we won't create schema with the reader
      await().atMost(10, TimeUnit.SECONDS).until {
        admin.schemas().getAllSchemas(DLQ_TOPIC).isNotEmpty()
      }

      await().atMost(10, TimeUnit.SECONDS).until {
        client
          .newReader(Schema.BYTES)
          .topic(DLQ_TOPIC)
          .subscriptionName("test-dlq-subscription")
          .startMessageId(MessageId.earliest)
          .create()
          .use { reader ->
            val readMsg = reader.readNext(1, TimeUnit.SECONDS)
            if (readMsg != null) {
              resultMsg = "\n\n\t\tSUCCESS Read message from DLQ: ${readMsg.value}\n"
              true
            } else {
              false
            }
          }
      }
      producer.close()
      consumer.close()
      client.close()
    }
  } catch (e: Exception) {
    log.error("Got exception: ", e)
  } finally {
    log.info(resultMsg)

    // Tear everything dangling down
    exitProcess(0)
  }
}

private fun provision(admin: PulsarAdmin) {
  admin
    .tenants()
    .createTenant(
      TENANT, TenantInfo.builder().allowedClusters(admin.clusters().clusters.toSet()).build()
    )
  await().atMost(10, TimeUnit.SECONDS).until { admin.tenants().tenants.contains(TENANT) }

  admin.namespaces().createNamespace(TENANT_NAMESPACE)
  await().atMost(10, TimeUnit.SECONDS).until {
    admin.namespaces().getNamespaces(TENANT).contains(TENANT_NAMESPACE)
  }

  provisionTopic(TOPIC, admin)
  provisionTopic(DLQ_TOPIC, admin)
  provisionTopic(RETRY_TOPIC, admin)
}

private fun provisionTopic(topic: String, admin: PulsarAdmin) {
  admin.topics().createNonPartitionedTopic(topic)
  await().atMost(10, TimeUnit.SECONDS).until {
    admin.topics().getList(TENANT_NAMESPACE).any { it.contains(topic) }
  }

  admin.namespaces().setSchemaValidationEnforced(TENANT_NAMESPACE, false)
  admin
    .topicPolicies()
    .setSchemaCompatibilityStrategy(topic, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE)

  await().atMost(10, TimeUnit.SECONDS).until {
    !admin.namespaces().getSchemaValidationEnforced(TENANT_NAMESPACE) &&
    admin.topicPolicies().getSchemaCompatibilityStrategy(topic, true) ==
    SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE
  }
}

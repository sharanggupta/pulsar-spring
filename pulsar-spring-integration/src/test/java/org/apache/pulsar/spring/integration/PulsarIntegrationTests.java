/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under this License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.spring.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.spring.core.DefaultPulsarConsumerFactory;
import org.apache.pulsar.spring.core.DefaultPulsarProducerFactory;
import org.apache.pulsar.spring.core.PulsarConsumerFactory;
import org.apache.pulsar.spring.core.PulsarTemplate;
import org.apache.pulsar.spring.integration.dsl.Pulsar;
import org.apache.pulsar.spring.integration.support.PulsarIntegrationHeaders;
import org.apache.pulsar.spring.listener.AckMode;
import org.apache.pulsar.spring.listener.Acknowledgement;
import org.apache.pulsar.spring.listener.DefaultPulsarMessageListenerContainer;
import org.apache.pulsar.spring.listener.PulsarContainerProperties;
import org.apache.pulsar.spring.support.PulsarHeaders;
import org.apache.pulsar.spring.support.PulsarNull;
import org.apache.pulsar.spring.test.support.PulsarTestContainerSupport;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Tests the Apache Pulsar channel adapters against a real broker.
 *
 * @since 3.0.0
 */
@SpringJUnitConfig
@DirtiesContext
class PulsarIntegrationTests implements PulsarTestContainerSupport {

	static final String ORDERS_TOPIC = "integration-orders";

	static final String MANUAL_ACK_TOPIC = "integration-manual-ack";

	static final String TOMBSTONES_TOPIC = "integration-tombstones";

	static final String SUBSCRIPTION = "integration-subscription";

	@Autowired
	MessageChannel toPulsar;

	@Autowired
	QueueChannel sendResults;

	@Autowired
	QueueChannel fromPulsar;

	@Autowired
	QueueChannel fromPulsarWithManualAck;

	@Autowired
	MessageChannel tombstonesToPulsar;

	@Autowired
	QueueChannel tombstonesFromPulsar;

	@Autowired
	PulsarTemplate<String> pulsarTemplate;

	@Test
	void messageSentByTheOutboundAdapterIsReceivedByTheMessageDrivenAdapter() {
		this.toPulsar.send(MessageBuilder.withPayload("hello")
			.setHeader(PulsarHeaders.KEY, "customer-1")
			.setHeader("trace-id", "abc")
			.build());

		Message<?> sent = this.sendResults.receive(30_000);
		assertThat(sent).isNotNull();
		assertThat(sent.getHeaders().get(PulsarHeaders.MESSAGE_ID)).isInstanceOf(MessageId.class);

		Message<?> received = this.fromPulsar.receive(30_000);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("hello");
		assertThat(received.getHeaders()).containsEntry(PulsarHeaders.KEY, "customer-1")
			.containsEntry("trace-id", "abc")
			.doesNotContainKey(PulsarIntegrationHeaders.ACKNOWLEDGMENT);
		assertThat((String) received.getHeaders().get(PulsarHeaders.TOPIC_NAME)).endsWith(ORDERS_TOPIC);
	}

	@Test
	void acknowledgementIsAvailableWhenTheContainerUsesTheManualAcknowledgementMode() throws Exception {
		this.pulsarTemplate.newMessage("needs-ack").withTopic(MANUAL_ACK_TOPIC).send();

		Message<?> received = this.fromPulsarWithManualAck.receive(30_000);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("needs-ack");
		assertThat(received.getHeaders().get(PulsarIntegrationHeaders.ACKNOWLEDGMENT))
			.isInstanceOf(Acknowledgement.class)
			.satisfies((acknowledgement) -> ((Acknowledgement) acknowledgement).acknowledge());
	}

	@Test
	void nullValueIsForwardedAsATombstoneWhenTheOutboundAdapterHasASchema() {
		this.tombstonesToPulsar
			.send(MessageBuilder.withPayload(PulsarNull.INSTANCE).setHeader(PulsarHeaders.KEY, "customer-1").build());

		Message<?> received = this.tombstonesFromPulsar.receive(30_000);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isSameAs(PulsarNull.INSTANCE);
		assertThat(received.getHeaders()).containsEntry(PulsarHeaders.KEY, "customer-1");
	}

	@Configuration
	@EnableIntegration
	static class Config {

		@Bean
		PulsarClient pulsarClient() throws PulsarClientException {
			PulsarClient client = PulsarClient.builder().serviceUrl(PULSAR_CONTAINER.getPulsarBrokerUrl()).build();
			// A topic without subscriptions discards its messages: create them before
			// sending
			for (String topic : new String[] { ORDERS_TOPIC, MANUAL_ACK_TOPIC, TOMBSTONES_TOPIC }) {
				client.newConsumer(Schema.STRING).topic(topic).subscriptionName(SUBSCRIPTION).subscribe().close();
			}
			return client;
		}

		@Bean
		PulsarTemplate<String> pulsarTemplate(PulsarClient pulsarClient) {
			return new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient, ORDERS_TOPIC));
		}

		@Bean
		PulsarConsumerFactory<String> pulsarConsumerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarConsumerFactory<>(pulsarClient, List.of());
		}

		@Bean
		DefaultPulsarMessageListenerContainer<String> ordersContainer(
				PulsarConsumerFactory<String> pulsarConsumerFactory) {

			return container(pulsarConsumerFactory, ORDERS_TOPIC, AckMode.RECORD);
		}

		@Bean
		DefaultPulsarMessageListenerContainer<String> manualAckContainer(
				PulsarConsumerFactory<String> pulsarConsumerFactory) {

			return container(pulsarConsumerFactory, MANUAL_ACK_TOPIC, AckMode.MANUAL);
		}

		@Bean
		DefaultPulsarMessageListenerContainer<String> tombstonesContainer(
				PulsarConsumerFactory<String> pulsarConsumerFactory) {

			return container(pulsarConsumerFactory, TOMBSTONES_TOPIC, AckMode.RECORD);
		}

		private static DefaultPulsarMessageListenerContainer<String> container(
				PulsarConsumerFactory<String> pulsarConsumerFactory, String topic, AckMode ackMode) {

			// A single String argument would be taken as a topic pattern: use setTopics()
			PulsarContainerProperties properties = new PulsarContainerProperties();
			properties.setTopics(Set.of(topic));
			properties.setSubscriptionName(SUBSCRIPTION);
			properties.setSchema(Schema.STRING);
			properties.setAckMode(ackMode);
			return new DefaultPulsarMessageListenerContainer<>(pulsarConsumerFactory, properties);
		}

		@Bean
		QueueChannel sendResults() {
			return new QueueChannel();
		}

		@Bean
		QueueChannel fromPulsar() {
			return new QueueChannel();
		}

		@Bean
		QueueChannel fromPulsarWithManualAck() {
			return new QueueChannel();
		}

		@Bean
		QueueChannel tombstonesFromPulsar() {
			return new QueueChannel();
		}

		@Bean
		IntegrationFlow toPulsarFlow(PulsarTemplate<String> pulsarTemplate) {
			return IntegrationFlow.from("toPulsar")
				.handle(Pulsar.outboundAdapter(pulsarTemplate)
					.topic(ORDERS_TOPIC)
					.sendSuccessChannelName("sendResults"))
				.get();
		}

		@Bean
		IntegrationFlow tombstonesToPulsarFlow(PulsarTemplate<String> pulsarTemplate) {
			// A null value has no type to resolve the schema from
			return IntegrationFlow.from("tombstonesToPulsar")
				.handle(Pulsar.outboundAdapter(pulsarTemplate).topic(TOMBSTONES_TOPIC).schema(Schema.STRING))
				.get();
		}

		@Bean
		IntegrationFlow tombstonesFromPulsarFlow(DefaultPulsarMessageListenerContainer<String> tombstonesContainer) {

			return IntegrationFlow.from(Pulsar.messageDrivenChannelAdapter(tombstonesContainer))
				.channel("tombstonesFromPulsar")
				.get();
		}

		@Bean
		IntegrationFlow fromPulsarFlow(DefaultPulsarMessageListenerContainer<String> ordersContainer) {
			return IntegrationFlow.from(Pulsar.messageDrivenChannelAdapter(ordersContainer))
				.channel("fromPulsar")
				.get();
		}

		@Bean
		IntegrationFlow fromPulsarWithManualAckFlow(DefaultPulsarMessageListenerContainer<String> manualAckContainer) {

			return IntegrationFlow.from(Pulsar.messageDrivenChannelAdapter(manualAckContainer))
				.channel("fromPulsarWithManualAck")
				.get();
		}

	}

}

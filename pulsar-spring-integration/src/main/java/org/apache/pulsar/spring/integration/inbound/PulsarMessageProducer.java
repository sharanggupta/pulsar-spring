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

package org.apache.pulsar.spring.integration.inbound;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.spring.integration.support.PulsarIntegrationHeaders;
import org.apache.pulsar.spring.listener.Acknowledgement;
import org.apache.pulsar.spring.listener.PulsarAcknowledgingMessageListener;
import org.apache.pulsar.spring.listener.PulsarMessageListenerContainer;
import org.apache.pulsar.spring.support.converter.PulsarRecordMessageConverter;
import org.apache.pulsar.spring.support.header.JsonPulsarHeaderMapper;
import org.apache.pulsar.spring.support.header.PulsarHeaderMapper;
import org.jspecify.annotations.Nullable;

import org.springframework.integration.IntegrationPatternType;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.util.Assert;

/**
 * A message-driven channel adapter that sends the messages that a
 * {@link PulsarMessageListenerContainer} receives from Apache Pulsar to a message
 * channel.
 * <p>
 * Each Pulsar message becomes a message with the value of the Pulsar message as its
 * payload, or {@link org.apache.pulsar.spring.support.PulsarNull#INSTANCE} when the value
 * is {@code null}, and the headers that the {@link #setHeaderMapper(PulsarHeaderMapper)
 * header mapper} creates, which include the
 * {@link org.apache.pulsar.spring.support.PulsarHeaders}. When the container uses the
 * {@link org.apache.pulsar.spring.listener.AckMode#MANUAL} acknowledgement mode, the
 * {@link Acknowledgement} is available in the
 * {@link PulsarIntegrationHeaders#ACKNOWLEDGMENT} header.
 * <p>
 * When sending a message to the output channel fails, an
 * {@link org.springframework.messaging.support.ErrorMessage} is sent to the
 * {@link #setErrorChannel(org.springframework.messaging.MessageChannel) error channel},
 * if any. Otherwise, the exception is thrown to the container, which applies its error
 * handling: its {@link org.apache.pulsar.spring.listener.PulsarConsumerErrorHandler}, if
 * any, or a negative acknowledgement so that the message is redelivered. The container
 * does not negatively acknowledge a message in the
 * {@link org.apache.pulsar.spring.listener.AckMode#MANUAL} acknowledgement mode, and
 * without an error handler it stops consuming when the exception reaches it, so use an
 * error channel or an error handler with this mode.
 * <p>
 * The adapter controls the lifecycle of the container: it disables its auto-startup and
 * starts and stops it together with the adapter, so that the container does not receive
 * messages before the downstream flow is ready.
 * <p>
 * The adapter supports the record listener containers, not the batch ones.
 *
 * @since 3.0.0
 * @see PulsarMessageListenerContainer
 */
public class PulsarMessageProducer extends MessageProducerSupport {

	private final PulsarMessageListenerContainer container;

	private PulsarHeaderMapper headerMapper = JsonPulsarHeaderMapper.builder().build();

	/**
	 * Create an instance that consumes the messages using the provided container. The
	 * auto-startup of the container is disabled, because the adapter starts it.
	 * @param container the container, for example a
	 * {@link org.apache.pulsar.spring.listener.DefaultPulsarMessageListenerContainer}.
	 */
	public PulsarMessageProducer(PulsarMessageListenerContainer container) {
		Assert.notNull(container, "'container' must not be null");
		this.container = container;
		// The adapter starts the container, when its own downstream flow is ready.
		this.container.setAutoStartup(false);
	}

	/**
	 * Set the mapper that converts the Pulsar message properties and metadata to the
	 * message headers. The default is a {@link JsonPulsarHeaderMapper}.
	 * @param headerMapper the mapper.
	 */
	public void setHeaderMapper(PulsarHeaderMapper headerMapper) {
		Assert.notNull(headerMapper, "'headerMapper' must not be null");
		this.headerMapper = headerMapper;
	}

	@Override
	public String getComponentType() {
		return "pulsar:message-driven-channel-adapter";
	}

	@Override
	public IntegrationPatternType getIntegrationPatternType() {
		return IntegrationPatternType.inbound_channel_adapter;
	}

	@Override
	protected void onInit() {
		super.onInit();
		this.container.setupMessageListener(new IntegrationMessageListener(this.headerMapper));
	}

	@Override
	protected void doStart() {
		this.container.start();
	}

	@Override
	protected void doStop() {
		this.container.stop();
	}

	@SuppressWarnings("serial")
	private final class IntegrationMessageListener implements PulsarAcknowledgingMessageListener<Object> {

		private final transient PulsarRecordMessageConverter<Object> converter;

		IntegrationMessageListener(PulsarHeaderMapper headerMapper) {
			this.converter = new PulsarRecordMessageConverter<>(headerMapper);
		}

		@Override
		public void received(Consumer<Object> consumer, Message<Object> record,
				@Nullable Acknowledgement acknowledgement) {

			org.springframework.messaging.Message<?> converted = this.converter.toMessage(record, consumer,
					Object.class);
			AbstractIntegrationMessageBuilder<?> builder = getMessageBuilderFactory()
				.withPayload(converted.getPayload())
				.copyHeaders(converted.getHeaders());
			if (acknowledgement != null) {
				builder.setHeader(PulsarIntegrationHeaders.ACKNOWLEDGMENT, acknowledgement);
			}
			sendMessage(builder.build());
		}

	}

}

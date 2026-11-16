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

package org.apache.pulsar.spring.integration.dsl;

import org.apache.pulsar.spring.core.PulsarOperations;
import org.apache.pulsar.spring.listener.PulsarMessageListenerContainer;

/**
 * Factory class for Apache Pulsar components.
 *
 * @since 3.0.0
 */
public final class Pulsar {

	/**
	 * Create a {@link PulsarMessageHandlerSpec} to send messages to Apache Pulsar.
	 * @param pulsarOperations the operations to send the messages with, such as a
	 * {@link org.apache.pulsar.spring.core.PulsarTemplate}.
	 * @param <T> the type of the values that are sent.
	 * @return the spec.
	 */
	public static <T> PulsarMessageHandlerSpec<T> outboundAdapter(PulsarOperations<T> pulsarOperations) {
		return new PulsarMessageHandlerSpec<>(pulsarOperations);
	}

	/**
	 * Create a {@link PulsarMessageProducerSpec} for a message-driven channel adapter
	 * that sends the messages received from Apache Pulsar to the channel.
	 * @param container the container that receives the messages.
	 * @return the spec.
	 */
	public static PulsarMessageProducerSpec messageDrivenChannelAdapter(PulsarMessageListenerContainer container) {
		return new PulsarMessageProducerSpec(container);
	}

	private Pulsar() {
	}

}

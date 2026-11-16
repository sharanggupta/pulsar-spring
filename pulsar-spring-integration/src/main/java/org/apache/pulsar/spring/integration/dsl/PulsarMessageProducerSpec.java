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

import org.apache.pulsar.spring.integration.inbound.PulsarMessageProducer;
import org.apache.pulsar.spring.listener.PulsarMessageListenerContainer;
import org.apache.pulsar.spring.support.header.PulsarHeaderMapper;

import org.springframework.integration.dsl.MessageProducerSpec;

/**
 * A {@link MessageProducerSpec} implementation for the {@link PulsarMessageProducer}.
 *
 * @since 3.0.0
 */
public class PulsarMessageProducerSpec extends MessageProducerSpec<PulsarMessageProducerSpec, PulsarMessageProducer> {

	PulsarMessageProducerSpec(PulsarMessageListenerContainer container) {
		super(new PulsarMessageProducer(container));
	}

	/**
	 * Configure the mapper of the Pulsar message properties and metadata to the message
	 * headers.
	 * @param headerMapper the mapper.
	 * @return the spec.
	 */
	public PulsarMessageProducerSpec headerMapper(PulsarHeaderMapper headerMapper) {
		this.target.setHeaderMapper(headerMapper);
		return this;
	}

}

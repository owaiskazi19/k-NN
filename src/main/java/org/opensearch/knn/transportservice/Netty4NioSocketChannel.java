/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.knn.transportservice;/*
                                            * SPDX-License-Identifier: Apache-2.0
                                            *
                                            * The OpenSearch Contributors require contributions made to
                                            * this file be licensed under the Apache-2.0 license or a
                                            * compatible open source license.
                                            */

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.channels.SocketChannel;

/**
 * Helper class to expose {@link #javaChannel()} method
 */
public class Netty4NioSocketChannel extends NioSocketChannel {

    public Netty4NioSocketChannel() {
        super();
    }

    public Netty4NioSocketChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    @Override
    public SocketChannel javaChannel() {
        return super.javaChannel();
    }

}

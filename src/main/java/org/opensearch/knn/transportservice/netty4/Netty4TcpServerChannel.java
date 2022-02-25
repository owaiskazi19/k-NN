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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.knn.transportservice.netty4;

import io.netty.channel.Channel;
import org.opensearch.action.ActionListener;
import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.transport.TcpServerChannel;

import java.net.InetSocketAddress;

public class Netty4TcpServerChannel implements TcpServerChannel {

    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    public Netty4TcpServerChannel(Channel channel) {
        this.channel = channel;
        Netty4TcpChannel.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public String toString() {
        return "Netty4TcpChannel{" + "localAddress=" + getLocalAddress() + '}';
    }
}

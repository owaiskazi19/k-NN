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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

final class OpenSearchLoggingHandler extends LoggingHandler {

    OpenSearchLoggingHandler() {
        super(LogLevel.TRACE);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // We do not want to log read complete events because we log inbound messages in the TcpTransport.
        ctx.fireChannelReadComplete();
    }
}

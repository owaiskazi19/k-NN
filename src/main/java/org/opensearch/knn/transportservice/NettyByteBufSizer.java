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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

@ChannelHandler.Sharable
public class NettyByteBufSizer extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        int readableBytes = buf.readableBytes();
        if (buf.capacity() >= 1024) {
            ByteBuf resized = buf.discardReadBytes().capacity(readableBytes);
            assert resized.readableBytes() == readableBytes;
            out.add(resized.retain());
        } else {
            out.add(buf.retain());
        }
    }
}

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

package org.opensearch.knn.transportservice;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.Writeable.Reader;
import org.opensearch.transport.*;
import org.opensearch.transport.TransportService;

/**
 * This interface allows plugins to intercept requests on both the sender and the receiver side.
 */
public interface TransportInterceptor {
    /**
     * This is called for each handler that is registered via
     * {@link org.opensearch.transport.TransportService#registerRequestHandler(String, String, boolean, boolean, Reader, TransportRequestHandler)} or
     * {@link org.opensearch.transport.TransportService#registerRequestHandler(String, String, Reader, TransportRequestHandler)}. The returned handler is
     * used instead of the passed in handler. By default the provided handler is returned.
     */
    default <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return actualHandler;
    }

    /**
     * This is called up-front providing the actual low level {@link AsyncSender} that performs the low level send request.
     * The returned sender is used to send all requests that come in via
     * {@link org.opensearch.transport.TransportService#sendRequest(DiscoveryNode, String, TransportRequest, TransportResponseHandler)} or
     * {@link TransportService#sendRequest(DiscoveryNode, String, TransportRequest, TransportRequestOptions, TransportResponseHandler)}.
     * This allows plugins to perform actions on each send request including modifying the request context etc.
     */
    default AsyncSender interceptSender(AsyncSender sender) {
        return sender;
    }

    /**
     * A simple interface to decorate
     * {@link #sendRequest(Transport.Connection, String, TransportRequest, TransportRequestOptions, TransportResponseHandler)}
     */
    interface AsyncSender {
        <T extends TransportResponse> void sendRequest(
            Transport.Connection connection,
            String action,
            TransportRequest request,
            TransportRequestOptions options,
            TransportResponseHandler<T> handler
        );
    }
}

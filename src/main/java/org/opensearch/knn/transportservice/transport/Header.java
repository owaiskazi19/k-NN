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

package org.opensearch.knn.transportservice.transport;

import org.opensearch.Version;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.*;

public class Header {

    private static final String RESPONSE_NAME = "NO_ACTION_NAME_FOR_RESPONSES";

    private final int networkMessageSize;
    private final Version version;
    private final long requestId;
    private final byte status;
    // These are directly set by tests
    String actionName;
    Tuple<Map<String, String>, Map<String, Set<String>>> headers;
    Set<String> features;

    Header(int networkMessageSize, long requestId, byte status, Version version) {
        this.networkMessageSize = networkMessageSize;
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    public Version getVersion() {
        return version;
    }

    public long getRequestId() {
        return requestId;
    }

    byte getStatus() {
        return status;
    }

    public boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    public boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }

    public boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    public boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }

    public String getActionName() {
        return actionName;
    }

    public boolean needsToReadVariableHeader() {
        return headers == null;
    }

    public Set<String> getFeatures() {
        return features;
    }

    public Tuple<Map<String, String>, Map<String, Set<String>>> getHeaders() {
        return headers;
    }

    void finishParsingHeader(StreamInput input) throws IOException {
        this.headers = ThreadContext.readHeadersFromStream(input);

        if (isRequest()) {
            final String[] featuresFound = input.readStringArray();
            if (featuresFound.length == 0) {
                features = Collections.emptySet();
            } else {
                features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(featuresFound)));
            }
            this.actionName = input.readString();
        } else {
            this.actionName = RESPONSE_NAME;
        }
    }

    @Override
    public String toString() {
        return "Header{"
            + networkMessageSize
            + "}{"
            + version
            + "}{"
            + requestId
            + "}{"
            + isRequest()
            + "}{"
            + isError()
            + "}{"
            + isHandshake()
            + "}{"
            + isCompressed()
            + "}{"
            + actionName
            + "}";
    }
}

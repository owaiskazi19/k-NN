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
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
 * layer, specifically for the same large buffer send to several nodes.
 */
public class BytesTransportRequest extends TransportRequest {

    BytesReference bytes;
    Version version;

    public BytesTransportRequest(StreamInput in) throws IOException {
        super(in);
        bytes = in.readBytesReference();
        version = in.getVersion();
    }

    public BytesTransportRequest(BytesReference bytes, Version version) {
        this.bytes = bytes;
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public BytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(bytes.length());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(bytes);
    }
}

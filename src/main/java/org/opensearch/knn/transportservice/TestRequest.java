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

package org.opensearch.knn.transportservice;//

// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.io.IOException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

public class TestRequest extends TransportRequest {
    String value;

    public TestRequest(String value) {
        this.value = value;
    }

    public TestRequest(StreamInput in) throws IOException {
        super(in);
        this.value = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.value);
    }
}

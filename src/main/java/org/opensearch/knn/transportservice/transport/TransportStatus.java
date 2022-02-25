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

public final class TransportStatus {

    private static final byte STATUS_REQRES = 1 << 0;
    private static final byte STATUS_ERROR = 1 << 1;
    private static final byte STATUS_COMPRESS = 1 << 2;
    private static final byte STATUS_HANDSHAKE = 1 << 3;

    public static boolean isRequest(byte value) {
        return (value & STATUS_REQRES) == 0;
    }

    public static byte setRequest(byte value) {
        value &= ~STATUS_REQRES;
        return value;
    }

    public static byte setResponse(byte value) {
        value |= STATUS_REQRES;
        return value;
    }

    public static boolean isError(byte value) {
        return (value & STATUS_ERROR) != 0;
    }

    public static byte setError(byte value) {
        value |= STATUS_ERROR;
        return value;
    }

    public static boolean isCompress(byte value) {
        return (value & STATUS_COMPRESS) != 0;
    }

    public static byte setCompress(byte value) {
        value |= STATUS_COMPRESS;
        return value;
    }

    static boolean isHandshake(byte value) { // pkg private since it's only used internally
        return (value & STATUS_HANDSHAKE) != 0;
    }

    static byte setHandshake(byte value) { // pkg private since it's only used internally
        value |= STATUS_HANDSHAKE;
        return value;
    }
}

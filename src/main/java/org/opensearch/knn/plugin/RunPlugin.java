/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin;

public class RunPlugin {

    public static void main(String[] args) {
        System.out.println("HELLO HELLO HELLO");
        KNNPlugin knnPlugin = new KNNPlugin();
        knnPlugin.additionalSettings();
    }

}

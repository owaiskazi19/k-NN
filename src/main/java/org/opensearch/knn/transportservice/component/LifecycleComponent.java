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

package org.opensearch.knn.transportservice.component;

import org.opensearch.common.component.Lifecycle;
import org.opensearch.common.component.LifecycleListener;
import org.opensearch.common.lease.Releasable;

public interface LifecycleComponent extends Releasable {

    Lifecycle.State lifecycleState();

    void addLifecycleListener(org.opensearch.common.component.LifecycleListener listener);

    void removeLifecycleListener(LifecycleListener listener);

    void start();

    void stop();
}

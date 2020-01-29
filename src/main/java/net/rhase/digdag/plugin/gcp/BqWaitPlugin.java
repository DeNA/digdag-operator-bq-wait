/* Licensed under Apache-2.0 */
package net.rhase.digdag.plugin.gcp;

import java.util.Collections;
import java.util.List;

import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;

public class BqWaitPlugin implements Plugin {

    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return BqWaitOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    private static class BqWaitOperatorProvider implements OperatorProvider {

        @Override
        public List<OperatorFactory> get() {
            return Collections.singletonList(new BqWaitOperatorFactory());
        }

    }
}

package cn.com.bronzebeard.paimon.flink.utils;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.paimon.annotation.Experimental;
import org.junit.jupiter.api.extension.*;

import java.util.Collections;
import java.util.function.Supplier;

/**
 * @author sherhomhuang
 * @date 2024/09/14 19:10
 * @description
 */
public final class MiniClusterWithClientExtension
        implements BeforeAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        AfterAllCallback,
        ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(MiniClusterWithClientExtension.class);

    private static final String CLUSTER_REST_CLIENT = "clusterRestClient";
    private static final String MINI_CLUSTER_CLIENT = "miniClusterClient";

    private final Supplier<MiniClusterResourceConfiguration>
            miniClusterResourceConfigurationSupplier;

    private InternalMiniClusterExtension internalMiniClusterExtension;
    private Configuration defaultUserConfig;

    public MiniClusterWithClientExtension(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration, Configuration defaultUserConfig) {
        this(() -> miniClusterResourceConfiguration);
        this.defaultUserConfig = defaultUserConfig;
    }

    @Experimental
    public MiniClusterWithClientExtension(
            Supplier<MiniClusterResourceConfiguration> miniClusterResourceConfigurationSupplier) {
        this.miniClusterResourceConfigurationSupplier = miniClusterResourceConfigurationSupplier;
    }

    // Accessors

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(InjectClusterClient.class)
                && ClusterClient.class.isAssignableFrom(parameterType)) {
            return true;
        }
        return internalMiniClusterExtension.supportsParameter(parameterContext, extensionContext);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(InjectClusterClient.class)) {
            if (parameterType.equals(RestClusterClient.class)) {
                return extensionContext
                        .getStore(NAMESPACE)
                        .getOrComputeIfAbsent(
                                CLUSTER_REST_CLIENT,
                                k -> {
                                    try {
                                        return new CloseableParameter<>(
                                                createRestClusterClient(
                                                        internalMiniClusterExtension));
                                    } catch (Exception e) {
                                        throw new ParameterResolutionException(
                                                "Cannot create rest cluster client", e);
                                    }
                                },
                                CloseableParameter.class)
                        .get();
            }
            // Default to MiniClusterClient
            return extensionContext
                    .getStore(NAMESPACE)
                    .getOrComputeIfAbsent(
                            MINI_CLUSTER_CLIENT,
                            k -> {
                                try {
                                    return new CloseableParameter<>(
                                            createMiniClusterClient(internalMiniClusterExtension));
                                } catch (Exception e) {
                                    throw new ParameterResolutionException(
                                            "Cannot create mini cluster client", e);
                                }
                            },
                            CloseableParameter.class)
                    .get();
        }
        return internalMiniClusterExtension.resolveParameter(parameterContext, extensionContext);
    }

    // Lifecycle implementation

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        internalMiniClusterExtension =
                new InternalMiniClusterExtension(miniClusterResourceConfigurationSupplier.get());
        internalMiniClusterExtension.beforeAll(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        registerEnv(internalMiniClusterExtension);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        unregisterEnv(internalMiniClusterExtension);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (internalMiniClusterExtension != null) {
            internalMiniClusterExtension.afterAll(context);
        }
    }

    // Implementation

    private void registerEnv(InternalMiniClusterExtension internalMiniClusterExtension) {
        final Configuration configuration =
                internalMiniClusterExtension.getMiniCluster().getConfiguration();

        final int defaultParallelism =
                configuration
                        .getOptional(CoreOptions.DEFAULT_PARALLELISM)
                        .orElse(internalMiniClusterExtension.getNumberSlots());

        TestEnvironment executionEnvironment =
                new TestEnvironment(
                        internalMiniClusterExtension.getMiniCluster(), defaultParallelism, false);
        executionEnvironment.setAsContext();
        TestStreamEnvironmentExt.setAsContext(
                internalMiniClusterExtension.getMiniCluster(), defaultParallelism, Collections.emptyList(), Collections.emptyList()
                , defaultUserConfig);
    }

    private void unregisterEnv(InternalMiniClusterExtension internalMiniClusterExtension) {
        TestStreamEnvironment.unsetAsContext();
        TestEnvironment.unsetAsContext();
    }

    private MiniClusterClient createMiniClusterClient(
            InternalMiniClusterExtension internalMiniClusterExtension) {
        return new MiniClusterClient(
                internalMiniClusterExtension.getClientConfiguration(),
                internalMiniClusterExtension.getMiniCluster());
    }

    private RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient(
            InternalMiniClusterExtension internalMiniClusterExtension) throws Exception {
        return new RestClusterClient<>(
                internalMiniClusterExtension.getClientConfiguration(),
                MiniClusterClient.MiniClusterId.INSTANCE);
    }

    public RestClusterClient<MiniClusterClient.MiniClusterId> createRestClusterClient()
            throws Exception {
        return createRestClusterClient(internalMiniClusterExtension);
    }

    // Utils

    public Configuration getClientConfiguration() {
        return internalMiniClusterExtension.getClientConfiguration();
    }

    private static class CloseableParameter<T extends AutoCloseable>
            implements ExtensionContext.Store.CloseableResource {
        private final T autoCloseable;

        CloseableParameter(T autoCloseable) {
            this.autoCloseable = autoCloseable;
        }

        public T get() {
            return autoCloseable;
        }

        @Override
        public void close() throws Throwable {
            this.autoCloseable.close();
        }
    }
}

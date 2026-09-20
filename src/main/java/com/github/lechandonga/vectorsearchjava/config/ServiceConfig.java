package com.github.lechandonga.vectorsearchjava.config;

import java.nio.file.Path;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 装配 EmbeddingGenerator / WalFile / VectorStore。
 * 存储的崩溃恢复由 {@link StoreInitializer} 在 Bean 就绪后触发。
 */
@Configuration
@EnableConfigurationProperties(VectorProperties.class)
public class ServiceConfig {

    @Bean
    public EmbeddingGenerator embeddingGenerator(VectorProperties props) {
        return new EmbeddingGenerator(props.getDimension());
    }

    @Bean
    public WalFile walFile(VectorProperties props) {
        return new WalFile(Path.of(props.getDataDir()));
    }

    @Bean
    public Snapshot.OverCapPolicy searchOverCapPolicy(VectorProperties props) {
        try {
            return Snapshot.OverCapPolicy.valueOf(props.getSearchOverCapPolicy().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "vector.service.search-over-cap-policy must be REJECT or DEGRADE, got: "
                            + props.getSearchOverCapPolicy(), e);
        }
    }

    @Bean
    public VectorStore vectorStore(EmbeddingGenerator embedding, WalFile walFile, VectorProperties props,
                                   Snapshot.OverCapPolicy searchOverCapPolicy) {
        return new VectorStore(embedding, walFile, props.getMaxVectors(),
                props.getSearchMaxCandidates(), searchOverCapPolicy, props.getCheckpointAfterOps());
    }
}

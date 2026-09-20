package com.github.lechandonga.vectorsearchjava.config;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;

/**
 * 在 Web 端口开放前执行 WAL/检查点恢复，保证对外服务时数据已加载完成。
 */
@Component
public class StoreInitializer {

    private final VectorStore store;

    public StoreInitializer(VectorStore store) {
        this.store = store;
    }

    @PostConstruct
    public void initialize() {
        store.init();
    }
}

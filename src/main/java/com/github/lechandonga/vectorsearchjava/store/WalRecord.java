package com.github.lechandonga.vectorsearchjava.store;

/**
 * WAL 记录。sealed 类型保证回放时穷尽处理。
 */
public sealed interface WalRecord permits WalRecord.Upsert, WalRecord.Delete {

    long version();

    /** 写入或幂等覆盖一条对象 */
    record Upsert(long version, String id, float[] vector, String source, String text,
                  java.util.List<String> tagKeys, java.util.List<String> tagValues) implements WalRecord {
        public Upsert {
            vector = vector == null ? null : vector.clone();
        }
    }

    /** 删除一条对象（删除长期有效：重建与重放后依然保持删除） */
    record Delete(long version, String id) implements WalRecord {}
}

package io.github.yanisderbikov.taskrelay;

public interface TaskPayloadCodec {

    String encode(Object payload);

    <P> P decode(String payload, Class<P> type);
}

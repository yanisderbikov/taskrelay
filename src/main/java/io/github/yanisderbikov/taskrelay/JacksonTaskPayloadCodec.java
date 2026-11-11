package io.github.yanisderbikov.taskrelay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

public final class JacksonTaskPayloadCodec implements TaskPayloadCodec {

    private final ObjectMapper objectMapper;

    public JacksonTaskPayloadCodec(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public String encode(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize task payload " + payload.getClass().getName(), e);
        }
    }

    @Override
    public <P> P decode(String payload, Class<P> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot deserialize task payload into " + type.getName(), e);
        }
    }
}

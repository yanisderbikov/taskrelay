package io.github.yanisderbikov.taskrelay;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import java.util.Objects;

public final class GsonTaskPayloadCodec implements TaskPayloadCodec {

    private final Gson gson;

    public GsonTaskPayloadCodec() {
        this(new Gson());
    }

    public GsonTaskPayloadCodec(Gson gson) {
        this.gson = Objects.requireNonNull(gson, "gson");
    }

    @Override
    public String encode(Object payload) {
        return gson.toJson(payload);
    }

    @Override
    public <P> P decode(String payload, Class<P> type) {
        try {
            return gson.fromJson(payload, type);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("Cannot deserialize task payload into " + type.getName(), e);
        }
    }
}

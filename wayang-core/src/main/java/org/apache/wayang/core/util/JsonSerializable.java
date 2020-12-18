package org.apache.incubator.wayang.core.util;

import org.json.JSONObject;
import org.apache.incubator.wayang.core.api.exception.WayangException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This interface prescribes implementing instances to be able to provide itself as a {@link JSONObject}. To allow
 * for deserialization, implementing class should furthermore provide a static {@code fromJson(JSONObject)} method.
 * <i>Note that it is recommended to use the {@link JsonSerializables} utility to class to handle serialization.</i>
 *
 * @see JsonSerializables
 */
public interface JsonSerializable {

    /**
     * Convert this instance to a {@link JSONObject}.
     *
     * @return the {@link JSONObject}
     */
    JSONObject toJson();

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    Serializer<JsonSerializable> uncheckedSerializer = new Serializer<>();

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    @SuppressWarnings("unchecked")
    static <T extends JsonSerializable> Serializer<T> uncheckedSerializer() {
        return (Serializer<T>) uncheckedSerializer;
    }

    /**
     * A {@link JsonSerializer} implementation to serialize {@link JsonSerializable}s.
     */
    class Serializer<T extends JsonSerializable> implements JsonSerializer<T> {

        @Override
        public JSONObject serialize(T serializable) {
            if (serializable == null) return null;
            return serializable.toJson();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T deserialize(JSONObject json, Class<? extends T> cls) {
            if (json == null || json.equals(JSONObject.NULL)) return null;
            try {
                final Method fromJsonMethod = cls.getMethod("fromJson", JSONObject.class);
                return (T) fromJsonMethod.invoke(null, json);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new WayangException(String.format("Could not execute %s.fromJson(...).", cls.getCanonicalName()), e);
            }
        }
    }

}
package io.github.yanisderbikov.taskrelay;

final class TaskTypes {

    private static final int MAX_LENGTH = 255;

    private TaskTypes() {
    }

    static String requireValid(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Task type must not be blank");
        }
        if (type.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("Task type is longer than " + MAX_LENGTH + " characters: " + type);
        }
        return type;
    }

    static String normalize(String key) {
        StringBuilder normalized = new StringBuilder(key.length());
        for (char c : key.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                normalized.append(Character.toLowerCase(c));
            }
        }
        return normalized.toString();
    }
}

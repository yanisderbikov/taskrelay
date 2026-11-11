package io.github.yanisderbikov.taskrelay;

import java.util.regex.Pattern;

final class TaskTable {

    private static final Pattern NAME = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*\\.)?[A-Za-z_][A-Za-z0-9_]*");

    private final String qualifiedName;
    private final String simpleName;

    TaskTable(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("taskrelay.table-name must be a plain or schema-qualified SQL identifier, got: " + name);
        }
        this.qualifiedName = name;
        int dot = name.indexOf('.');
        this.simpleName = dot < 0 ? name : name.substring(dot + 1);
    }

    String name() {
        return qualifiedName;
    }

    String indexName(String suffix) {
        return simpleName + "_" + suffix;
    }
}

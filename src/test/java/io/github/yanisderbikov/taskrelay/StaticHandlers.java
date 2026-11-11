package io.github.yanisderbikov.taskrelay;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.core.ResolvableType;

final class StaticHandlers {

    private StaticHandlers() {
    }

    @SuppressWarnings("unchecked")
    static TaskRegistry registry(TaskHandler<?>... handlers) {
        StaticListableBeanFactory factory = new StaticListableBeanFactory();
        for (int i = 0; i < handlers.length; i++) {
            factory.addBean("handler" + i, handlers[i]);
        }
        ObjectProvider<?> provider = factory.getBeanProvider(ResolvableType.forClass(TaskHandler.class));
        return new TaskRegistry((ObjectProvider<TaskHandler<?>>) provider);
    }
}

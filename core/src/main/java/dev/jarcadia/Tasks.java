package dev.jarcadia;

import java.util.LinkedList;
import java.util.List;

public class Tasks {

    private final List<Task.Builder> tasks;

    public static Tasks create() {
        return new Tasks();
    }

    private Tasks() {
        this.tasks = new LinkedList<>();
    }

    public Task.Builder add(String route) {
        Task.Builder task = new Task.Builder(route);
        tasks.add(task);
        return task;
    }

    public void add(Task.Builder task) {
        this.tasks.add(task);
    }

    protected List<Task.Builder> getTasks() {
        return tasks;
    }


}

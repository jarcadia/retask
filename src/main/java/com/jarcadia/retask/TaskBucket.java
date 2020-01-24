package com.jarcadia.retask;

import java.util.LinkedList;
import java.util.List;

public class TaskBucket {
	
	private final List<Task> tasks;
	
	protected TaskBucket() {
		this.tasks = new LinkedList<>();
	}
	
	protected void add(Task task) {
		this.tasks.add(task);
	}
	
	protected List<Task> getTasks() {
		return this.tasks;
	}
}

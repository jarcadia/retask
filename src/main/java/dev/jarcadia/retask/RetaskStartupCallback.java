package dev.jarcadia.retask;

import dev.jarcadia.redao.RedaoCommando;

@FunctionalInterface
public interface RetaskStartupCallback {
	void onStartup(RedaoCommando rcommando, Retask retask);
}

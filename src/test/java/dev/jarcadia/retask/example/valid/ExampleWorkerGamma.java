package dev.jarcadia.retask.example.valid;

import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerGamma {
    
    public void primaryHandler() {}

    @RetaskHandler(value = "gamma")
    public void handler() {}

}

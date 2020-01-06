package com.jarcadia.retask.example.valid;

import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerGamma {
    
    public void primaryHandler() {}

    @RetaskHandler(value = "gamma")
    public void handler() {}

}

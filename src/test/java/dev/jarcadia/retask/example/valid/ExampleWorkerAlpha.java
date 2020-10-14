package dev.jarcadia.retask.example.valid;

import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerAlpha {
    
    @RetaskHandler("alpha")
    public void handler() {}

    @RetaskHandler("alpha.one")
    public void handlerOne() {}

}

package dev.jarcadia.retask.example.valid;

import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerBeta {
    
    @RetaskHandler("beta.one")
    public void handlerOne() {}

    @RetaskHandler("beta.two")
    public void handlerTwo() {}

    public void handlerThree() {}
}

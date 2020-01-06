package com.jarcadia.retask.example.valid;

import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerBeta {
    
    @RetaskHandler("beta.one")
    public void handlerOne() {}

    @RetaskHandler("beta.two")
    public void handlerTwo() {}

    public void handlerThree() {}
}

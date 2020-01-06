package com.jarcadia.retask.example.valid;

import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerAlpha {
    
    @RetaskHandler("alpha")
    public void handler() {}

    @RetaskHandler("alpha.one")
    public void handlerOne() {}

}

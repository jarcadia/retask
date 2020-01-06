package com.jarcadia.retask.example.valid;

import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerAlphaCopy {
    
    @RetaskHandler("alpha.one")
    public void differentHandler() {}

}

package dev.jarcadia.retask.example.valid;

import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExampleWorkerAlphaCopy {
    
    @RetaskHandler("alpha.one")
    public void differentHandler() {}

}

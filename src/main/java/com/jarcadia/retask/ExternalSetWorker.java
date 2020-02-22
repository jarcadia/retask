package com.jarcadia.retask;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.Dao;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExternalSetWorker {

    private final Logger logger = LoggerFactory.getLogger(ExternalSetWorker.class);
	

	@RetaskHandler("dao.set")
	public void externalSet(Dao dao, Map<String, Object> values) {
		logger.info("Updating {} with {}", dao, values);
		dao.set(values);
	}
}

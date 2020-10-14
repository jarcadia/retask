package dev.jarcadia.retask;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.jarcadia.redao.Dao;
import dev.jarcadia.retask.annontations.RetaskHandler;
import dev.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class ExternalSetWorker {

    private final Logger logger = LoggerFactory.getLogger(ExternalSetWorker.class);

	@RetaskHandler("dao.set")
	public void externalSet(Dao dao, Map<String, Object> values) {
		logger.info("Updating {} with {}", dao, values);
		dao.setAll(values);
	}
}

package data.analyzer.service;

import data.analyzer.model.Data;


public interface KafkaDataService {

    void handle(Data data);
}

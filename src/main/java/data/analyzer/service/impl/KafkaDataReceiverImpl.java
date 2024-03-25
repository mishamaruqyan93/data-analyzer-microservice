package data.analyzer.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import data.analyzer.config.LocalDateTimeDeserializer;
import data.analyzer.model.Data;
import data.analyzer.service.KafkaDataReceiver;
import data.analyzer.service.KafkaDataService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class KafkaDataReceiverImpl implements KafkaDataReceiver {

    private final LocalDateTimeDeserializer localDateTimeDeserializer;
    private final KafkaReceiver<String, Object> kafkaReceiver;
    private final KafkaDataService kafkaDataService;

    @PostConstruct
    public void init() {
        fetch();
    }

    @Override
    public void fetch() {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, localDateTimeDeserializer)
                .create();

        kafkaReceiver.receive()
                .subscribe(r -> {
                    Data data = gson.fromJson(r.value().toString(), Data.class);
                    kafkaDataService.handle(data);
                    r.receiverOffset().acknowledge();
                });
    }

}

package com.jozsef.kafkademoconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jozsef.kafkademoconsumer.entity.LibraryEvent;
import com.jozsef.kafkademoconsumer.repository.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {}", libraryEvent);

        if (libraryEvent.getId() != null && libraryEvent.getId() == 0) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getType()) {
            case NEW:
                // save operation
                save(libraryEvent);
                break;
            case UPDATE:
                // validate the library event
                validate(libraryEvent);
                // save operation
                save(libraryEvent);
                break;
            default:
                log.info("Invalid library event type");
        }
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        Integer key = consumerRecord.key();
        String value = consumerRecord.value();
        ListenableFuture<SendResult<Integer, String>> result = kafkaTemplate.sendDefault(key, value);
        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
                handleSuccess(key, value, integerStringSendResult);
            }
        });
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getId() == null) {
            throw new IllegalArgumentException("Library event id should not be null");
        }

        Optional<LibraryEvent> libraryEventOptional = this.libraryEventsRepository.findById(libraryEvent.getId());
        if (!libraryEventOptional.isPresent()) {
            log.info("Invalid library event : {}", libraryEvent);
            throw new IllegalArgumentException("Invalid library event");
        }

        log.info("Validation successful for the library event : {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        LibraryEvent savedLibraryEvent = this.libraryEventsRepository.save(libraryEvent);
        log.info("Successfully saved the library event {}", savedLibraryEvent);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message with key {}, exception is {}", key, ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in onFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully for key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }

}

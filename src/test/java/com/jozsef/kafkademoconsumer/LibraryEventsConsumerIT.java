package com.jozsef.kafkademoconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jozsef.kafkademoconsumer.consumer.LibraryEventsConsumer;
import com.jozsef.kafkademoconsumer.entity.Book;
import com.jozsef.kafkademoconsumer.entity.LibraryEvent;
import com.jozsef.kafkademoconsumer.entity.LibraryEventType;
import com.jozsef.kafkademoconsumer.repository.LibraryEventsRepository;
import com.jozsef.kafkademoconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewAndBadLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":null,\"type\":\"NEW\",\"book\":{\"id\":123,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        String jsonBad = "{\"id\":null,\"type\":\"UPDATE\",\"book\":{\"id\":12,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";

        // when
        kafkaTemplate.sendDefault(json).get();
        kafkaTemplate.sendDefault(jsonBad).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(2)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(2)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEvents.size() == 1;
        libraryEvents.forEach(libraryEvent -> {
            assert libraryEvent.getId() != null;
            assertEquals(123, libraryEvent.getBook().getId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        String json = "{\"id\":null,\"type\":\"NEW\",\"book\":{\"id\":12,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        Book book = Book.builder()
                .id(123)
                .author("Dilip")
                .name("Kafka Using Spring Boot 2nd Edition")
                .build();
        libraryEvent.setBook(book);
        libraryEvent.setType(LibraryEventType.UPDATE);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        // when
        kafkaTemplate.sendDefault(libraryEvent.getId(), updatedJson).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getId()).get();
        assertEquals("Kafka Using Spring Boot 2nd Edition", persistedLibraryEvent.getBook().getName());
    }

    @Test
    void publishInvalidLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":null,\"type\":\"MODIFY\",\"book\":{\"id\":12,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";

        // when
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEvents.isEmpty();
    }

    @Test
    void publishRetryableLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"id\":0,\"type\":\"UPDATE\",\"book\":{\"id\":12,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";

        // when
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEvents.isEmpty();
    }
}

package com.chip1stop.kafkapc;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UserConsumer {

    private static final Logger log = LoggerFactory.getLogger(UserConsumer.class);

    private List<User> bufferList = new ArrayList<>();

    private final UserRepository userRepository;

    @KafkaListener(topics = "test", groupId = "test-cc")
    public void receiveUser(
            @Payload byte[] message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Message received with key {}, partition {}, offset {}", key, partition, offset);

        try {
            User user = deserializeUserList(message);
            if (user != null) {
                bufferList.add(user);

                if (user.getName().equals("HieuPOC")) {
                    userRepository.saveAll(bufferList.subList(0, bufferList.size() - 1));
                    bufferList.clear();
                }

            } else {
                log.error("Failed to deserialize message");
            }
        } catch (Exception e) {
            log.error("Exception while processing message: {}", e.getMessage());
        }
    }

    private User deserializeUserList(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            // Deserialize the byte array back into List<User>
            return (User) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error("Deserialization error: ", e);
            return null;
        }
    }
}

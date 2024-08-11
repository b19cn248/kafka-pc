package com.chip1stop.kafkapc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.UUID;

@RestController
public class TestController {

    private final KafkaProducerV1<String, byte[]> producer;

    @Autowired
    public TestController(KafkaProducerV1<String, byte[]> producer) {
        this.producer = producer;
    }

    @GetMapping("/events")
    public String sendMessage() {

        List<User> users = UserGenerator.generateFakeUsers(10);
        User userMarker = new User(UUID.randomUUID().toString(), "HieuPOC", 0);

        for (User user : users) {
            System.out.println("\u001B[32m" + user + "\u001B[0m");
            byte[] userBytes = serializeUser(user);
            producer.send("test", user.getId(), userBytes);
        }

        producer.send("test", userMarker.getId(), serializeUser(userMarker));


        return "message published!";
    }

    private byte[] serializeUser(User user) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(user);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialization error", e);
        }
    }
}


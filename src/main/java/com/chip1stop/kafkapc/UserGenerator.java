package com.chip1stop.kafkapc;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UserGenerator {


    public static List<User> generateFakeUsers(int count) {
        List<User> users = new ArrayList<>();
        Faker faker = new Faker();

        for (int i = 0; i < count; i++) {
            String id = UUID.randomUUID().toString();
            String name = faker.name().fullName();
            Integer age = faker.number().numberBetween(18, 65);

            users.add(new User(id, name, age));
        }

        return users;
    }

    public static String generateFullName() {
        Faker faker = new Faker();
        return faker.name().fullName();
    }

}


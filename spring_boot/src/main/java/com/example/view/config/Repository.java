package com.example.view.config;

import org.springframework.data.jpa.repository.JpaRepository;

public interface Repository extends JpaRepository<DBSchema, Long> {
}

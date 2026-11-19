package com.example.demo.repository;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import com.example.demo.entity.OutboxEvent;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {
	List<OutboxEvent> findByEnviadoFalse();
}

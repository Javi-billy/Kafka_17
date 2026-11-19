package com.example.demo.services;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.example.demo.avro.PedidoAvro;
import com.example.demo.dto.PedidoDTO;

@Service
public class NotificacionConsumer {

	@Value("${server.port}")
	private String puerto;

	// Simula una tabla en BD de "eventos_procesados" para garantizar Idempotencia
    private final Set<String> pedidosProcesados = ConcurrentHashMap.newKeySet();
    
	
	// 1 intento original + 2 reintentos
	@RetryableTopic(attempts = "3", backOff = @BackOff(delay = 2000, multiplier = 2.0), dltStrategy = DltStrategy.FAIL_ON_ERROR)
	// @KafkaListener(topics = "pedidos-topic", groupId = "grupo-notificaciones")
	//@KafkaListener(topics = "pedidos-escalables-topic", groupId = "grupo-notificaciones")
	@KafkaListener(topics = "pedidos-avro-topic", groupId = "grupo-notificaciones")	
	public void procesarPedido(PedidoAvro  pedido, @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
			@Header(KafkaHeaders.OFFSET) long offset, Acknowledgment ack) {
		// System.out.println("Intentando procesar pedido: " + pedido.id());
		System.out.println("[Instancia puerto " + puerto + "] -> Partición: " + particion + " | Offset: " + offset
				+ " | Pedido: " + pedido.getId().toString());

		// 1. COMPROBACIÓN DE IDEMPOTENCIA
        if (pedidosProcesados.contains(pedido.getId().toString())) {
            System.out.println("IDEMPOTENCIA -> El pedido " + pedido.getId().toString() + " ya fue procesado antes. Ignorando duplicado.");
            ack.acknowledge(); // Confirmamos el offset para avanzar la cola sin repetir trabajo
            return;
        }
        
     // 2. SIMULACIÓN DE LÓGICA DE NEGOCIO (ej. Guardar en BD y cobrar)
        if ("FALLO-RED".equals(pedido.getId().toString())) {
            System.err.println("ERROR DE RED -> No hacemos ack.acknowledge(). El mensaje quedará sin confirmar.");
            // Al no llamar a ack.acknowledge(), Kafka no avanza el offset
            return; 
        }
		
        pedidosProcesados.add(pedido.getId().toString());
        System.out.println("ÉXITO -> Notificación enviada para pedido: " + pedido.getId().toString());

        // 3. CONFIRMACIÓN MANUAL DEL OFFSET
        ack.acknowledge();
        System.out.println("COMMIT MANUAL -> Offset " + offset + " confirmado en Kafka.");
        
		/*if (pedido.precio() <= 0) {
			throw new IllegalArgumentException("El precio del pedido no puede ser 0 o negativo: " + pedido.precio());
		}
		System.out.println("Evento procesado con éxito -> Pedido: " + pedido.producto());*/
	}

	@DltHandler
	public void manejarErrorEnDlt(PedidoAvro pedido, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
		//System.err.println("ALERTA DLT -> Pedido enviado a " + topic + " tras fallar todos los reintentos: " + pedido.id());
		System.err.println("[Instancia puerto " + puerto + "] ALERTA DLT -> Pedido enviado a " + topic + ": " + pedido.getId().toString());
	}
}
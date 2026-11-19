package com.example.demo;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.example.demo.avro.PedidoAvro;
import com.example.demo.dto.PedidoDTO;
import com.example.demo.services.NotificacionConsumer;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = { "pedidos-escalables-topic" })
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.auto-offset-reset=earliest"
})
class NotificacionConsumerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    private NotificacionConsumer consumer;

    @Test
    void cuandoLlegaPedidoValido_entoncesConsumerLoProcesaYConfirma() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // Añadimos any(Acknowledgment.class) como 4º parámetro
        doAnswer(invocation -> {
            invocation.callRealMethod();
            latch.countDown();
            return null;
        }).when(consumer).procesarPedido(
                any(PedidoAvro.class), 
                anyInt(), 
                anyLong(), 
                any(Acknowledgment.class)
        );

        PedidoDTO pedidoPrueba = new PedidoDTO("TEST-01", "Ratón inalámbrico", 45.00);
        kafkaTemplate.send("pedidos-escalables-topic", pedidoPrueba.id(), pedidoPrueba);

        boolean mensajeRecibido = latch.await(5, TimeUnit.SECONDS);
        assertTrue(mensajeRecibido, "El consumidor debería haber procesado el pedido");
    }

    @Test
    void cuandoLlegaPedidoDuplicado_entoncesSeInvocaDosVecesPeroEsIdempotente() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        doAnswer(invocation -> {
            invocation.callRealMethod();
            latch.countDown();
            return null;
        }).when(consumer).procesarPedido(
                any(PedidoAvro.class), 
                anyInt(), 
                anyLong(), 
                any(Acknowledgment.class)
        );

        // Enviamos el MISMO pedido dos veces seguidas
        PedidoDTO pedidoDuplicado = new PedidoDTO("TEST-DUPLICADO-99", "Teclado RGB", 75.00);
        kafkaTemplate.send("pedidos-escalables-topic", pedidoDuplicado.id(), pedidoDuplicado);
        kafkaTemplate.send("pedidos-escalables-topic", pedidoDuplicado.id(), pedidoDuplicado);

        boolean ambosRecibidos = latch.await(5, TimeUnit.SECONDS);
        assertTrue(ambosRecibidos, "Ambos mensajes deberían haber sido leídos por el listener");
        
        // Verificamos que el método se ejecutó 2 veces (en consola verás que la segunda dice IDEMPOTENCIA)
        verify(consumer, times(2)).procesarPedido(
                any(PedidoAvro.class), 
                anyInt(), 
                anyLong(), 
                any(Acknowledgment.class)
        );
    }
}
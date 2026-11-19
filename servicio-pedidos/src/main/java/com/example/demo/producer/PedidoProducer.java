package com.example.demo.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.demo.avro.PedidoAvro;
import com.example.demo.dto.PedidoDTO;

@Service
public class PedidoProducer {

	//private final KafkaTemplate<String, PedidoDTO> kafkaTemplate;
	private final KafkaTemplate<String, PedidoAvro> kafkaTemplate;

	public PedidoProducer(KafkaTemplate<String, PedidoAvro> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarPedido(PedidoDTO dto) {
        PedidoAvro eventoAvro = PedidoAvro.newBuilder()
                .setId(dto.id())
                .setProducto(dto.producto())
                .setPrecio(dto.precio())
                .build();

        kafkaTemplate.send("pedidos-avro-topic", eventoAvro.getId().toString(), eventoAvro);
    }
	
	/*public PedidoProducer(KafkaTemplate<String, PedidoDTO> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void enviarPedido(PedidoDTO pedido) {
		kafkaTemplate.send("pedidos-escalables-topic", pedido.id(), pedido);
	}*/
}

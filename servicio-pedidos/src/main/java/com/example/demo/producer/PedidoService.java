package com.example.demo.producer;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.demo.dto.PedidoDTO;
import com.example.demo.entity.OutboxEvent;
import com.example.demo.repository.OutboxRepository;

@Service
public class PedidoService {

	private final OutboxRepository outboxRepository;

	public PedidoService(OutboxRepository outboxRepository) {
		this.outboxRepository = outboxRepository;
	}

	@Transactional
	public void registrarPedido(PedidoDTO dto) {
		// Aquí guardarías también en tu PedidoRepository real

		// Guardamos el evento en la misma transacción local
		OutboxEvent evento = new OutboxEvent();
		evento.setPedidoId(dto.id());
		evento.setProducto(dto.producto());
		evento.setPrecio(dto.precio());
		outboxRepository.save(evento);

		System.out.println("BD -> Pedido y OutboxEvent guardados atómicamente: " + dto.id());
	}
}

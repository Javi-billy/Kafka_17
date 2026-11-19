package com.example.demo.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.PedidoDTO;
import com.example.demo.producer.PedidoProducer;
import com.example.demo.producer.PedidoService;

@RestController
@RequestMapping("/pedidos")
public class PedidosController {

	// private final PedidoProducer pedidoProducer;
	private final PedidoService pedidoService;

	public PedidosController(PedidoService pedidoService) {
		this.pedidoService = pedidoService;
	}

	@PostMapping
	public ResponseEntity<String> crearPedido(@RequestBody PedidoDTO pedido) {
		pedidoService.registrarPedido(pedido);
		return ResponseEntity.status(HttpStatus.CREATED)
				.body("Pedido registrado en BD de forma segura: " + pedido.id());
	}
}

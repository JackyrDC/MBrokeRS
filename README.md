# Integración de Sistemas de Mensajería (RabbitMQ & ActiveMQ)

Este proyecto implementa una arquitectura desacoplada donde dos sistemas desarrollados en lenguajes distintos intercambian mensajes a través de dos brokers de mensajería diferentes, unidos por un integrador bidireccional.

## Arquitectura
- **Sistema A:** Desarrollado en Rust, conectado a RabbitMQ vía AMQP.
- **Sistema B:** Desarrollado en C# (.NET 8), conectado a ActiveMQ vía STOMP.
- **Integrador:** Desarrollado en Python, traduce y enruta mensajes entre ambos brokers manejando hilos concurrentes.
- **Brokers:** RabbitMQ y ActiveMQ Classic, dockerizados con límites de recursos para eficiencia.

## Requisitos Previos
- Docker y Docker Compose instalados.
- Puertos locales libres: 5672, 15672 (RabbitMQ) y 61613, 8161 (ActiveMQ).

## Instrucciones de Ejecución
1. Clonar el repositorio y navegar a la raíz del proyecto.
2. Construir las imágenes y levantar la infraestructura en segundo plano:
   `docker-compose up -d --build`
3. Para observar la trazabilidad y la prueba de concepto en tiempo real, revisar los logs del integrador:
   `docker-compose logs -f integrador`
4. Para apagar los servicios y limpiar los volúmenes de datos temporales:
   `docker-compose down -v`
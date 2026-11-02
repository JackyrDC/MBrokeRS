import os
import time
import json
import functools
import pika
import stomp

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
ACTIVEMQ_HOST = os.getenv("ACTIVEMQ_HOST", "localhost")

COLA_RABBIT_A_B = "flujo_a_hacia_b"
COLA_RABBIT_B_A = "flujo_b_hacia_a"
COLA_ACTIVE_A_B = "/queue/flujo_a_hacia_b"
COLA_ACTIVE_B_A = "/queue/flujo_b_hacia_a"

def registrar_trazabilidad(body_str, fase, broker_origen):
    try:
        data = json.loads(body_str)
        msg_id = data.get("id", "UNKNOWN")
        origen = data.get("origen", "UNKNOWN")
        
        if fase == "RECIBIDO":
            print(f"[{fase}] ID: {msg_id} leido desde {broker_origen}")
        elif fase == "PROCESADO":
            print(f"[{fase}] ID: {msg_id} validado de {origen}")
        elif fase == "REENVIADO":
            print(f"[{fase}] ID: {msg_id} inyectado exitosamente hacia destino")
    except json.JSONDecodeError:
        print(f"[ERROR] Payload no es JSON válido: {body_str}")

class ActiveMQListener(stomp.ConnectionListener):
    def __init__(self, pika_conn, pika_channel, stomp_conn):
        self.pika_conn = pika_conn
        self.pika_channel = pika_channel
        self.stomp_conn = stomp_conn

    def on_message(self, frame):
        mensaje = frame.body
        registrar_trazabilidad(mensaje, "RECIBIDO", "ActiveMQ")
        registrar_trazabilidad(mensaje, "PROCESADO", "ActiveMQ")
        
        cb = functools.partial(
            self.pika_channel.basic_publish,
            exchange='',
            routing_key=COLA_RABBIT_B_A,
            body=mensaje
        )
        self.pika_conn.add_callback_threadsafe(cb)
        registrar_trazabilidad(mensaje, "REENVIADO", "ActiveMQ")
        self.stomp_conn.ack(frame.headers['message-id'], frame.headers['subscription'])

def on_rabbitmq_message(ch, method, properties, body, stomp_conn):
    mensaje = body.decode('utf-8')
    registrar_trazabilidad(mensaje, "RECIBIDO", "RabbitMQ")
    registrar_trazabilidad(mensaje, "PROCESADO", "RabbitMQ")
    
    stomp_conn.send(body=mensaje, destination=COLA_ACTIVE_A_B)
    registrar_trazabilidad(mensaje, "REENVIADO", "RabbitMQ")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    print("Iniciando Integrador...")
    
    stomp_conn = None
    pika_conn = None
    pika_channel = None
    
    while True:
        try:
            print("Intentando conectar a ActiveMQ y RabbitMQ...")
            stomp_conn = stomp.Connection([(ACTIVEMQ_HOST, 61613)])
            stomp_conn.connect('admin', 'admin', wait=True)
            
            credenciales = pika.PlainCredentials('user', 'password')
            pika_params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credenciales)
            pika_conn = pika.BlockingConnection(pika_params)
            pika_channel = pika_conn.channel()
            
            print("Conexión exitosa a ambos brokers.")
            break 
        except Exception as e:
            print(f"[ESPERA] Brokers no listos aún: {e}")
            print("Reintentando en 5 segundos...")
            time.sleep(5)
    
    pika_channel.queue_declare(queue=COLA_RABBIT_A_B)
    pika_channel.queue_declare(queue=COLA_RABBIT_B_A)
    
    listener = ActiveMQListener(pika_conn, pika_channel, stomp_conn)
    stomp_conn.set_listener('', listener)
    stomp_conn.subscribe(destination=COLA_ACTIVE_B_A, id=1, ack='client')
    
    on_message_callback = functools.partial(on_rabbitmq_message, stomp_conn=stomp_conn)
    pika_channel.basic_consume(queue=COLA_RABBIT_A_B, on_message_callback=on_message_callback)

    print("¡Integrador conectado y trazabilidad activada!")
    try:
        pika_channel.start_consuming()
    except KeyboardInterrupt:
        pika_channel.stop_consuming()
        stomp_conn.disconnect()
        pika_conn.close()

if __name__ == '__main__':
    main()
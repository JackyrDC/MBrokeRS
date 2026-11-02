use chrono::Utc;
use futures_lite::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct Contenido {
    mensaje: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct MessagePayload {
    id: String,
    fecha: String,
    origen: String,
    destino: String,
    operacion: String,
    contenido: Contenido,
}

#[tokio::main]
async fn main() -> lapin::Result<()> {
    let addr = "amqp://user:password@rabbitmq:5672/%2f";
    println!("Iniciando Sistema A...");
    
    // Bucle de resiliencia para la conexión
    let conn = loop {
        println!("Intentando conectar a RabbitMQ...");
        match Connection::connect(addr, ConnectionProperties::default()).await {
            Ok(c) => {
                println!("¡Conexión exitosa a RabbitMQ!");
                break c;
            }
            Err(e) => {
                println!("[ESPERA] RabbitMQ no está listo: {:?}. Reintentando en 5s...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    };
    
    let channel_produce = conn.create_channel().await?;
    let channel_consume = conn.create_channel().await?;

    let cola_salida = "flujo_a_hacia_b";
    let cola_entrada = "flujo_b_hacia_a";

    channel_produce.queue_declare(cola_salida.into(), QueueDeclareOptions::default(), FieldTable::default()).await?;
    channel_consume.queue_declare(cola_entrada.into(), QueueDeclareOptions::default(), FieldTable::default()).await?;

    let mut consumer = channel_consume
        .basic_consume(cola_entrada.into(), "sistema_a_consumer".into(), BasicConsumeOptions::default(), FieldTable::default())
        .await?;

    tokio::spawn(async move {
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    if let Ok(texto) = String::from_utf8(delivery.data.clone()) {
                        match serde_json::from_str::<MessagePayload>(&texto) {
                            Ok(payload) => {
                                println!("[RECIBIDO] ID: {} | Origen: {} | Operación: {}", payload.id, payload.origen, payload.operacion);
                                println!("[PROCESADO] Mensaje: {}", payload.contenido.mensaje);
                            }
                            Err(_) => println!("[ERROR] JSON inválido recibido: {}", texto),
                        }
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                }
                Err(e) => println!("Error al consumir: {:?}", e),
            }
        }
    });

    loop {
        let payload = MessagePayload {
            id: format!("MSG-{}", Uuid::new_v4().to_string().chars().take(8).collect::<String>().to_uppercase()),
            fecha: Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            origen: "Sistema-A".to_string(),
            destino: "Sistema-B".to_string(),
            operacion: "REGISTRO".to_string(),
            contenido: Contenido {
                mensaje: "Prueba de integración desde Rust".to_string(),
            },
        };

        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        channel_produce
            .basic_publish("".into(), cola_salida.into(), BasicPublishOptions::default(), &payload_bytes, BasicProperties::default())
            .await?;

        println!("[ENVIADO] ID: {} hacia {}", payload.id, payload.destino);
        sleep(Duration::from_secs(5)).await;
    }
}
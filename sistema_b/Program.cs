using System;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.ActiveMQ;

namespace sistema_b
{
    public class Contenido
    {
        public string mensaje { get; set; }
    }

    public class MessagePayload
    {
        public string id { get; set; }
        public string fecha { get; set; }
        public string origen { get; set; }
        public string destino { get; set; }
        public string operacion { get; set; }
        public Contenido contenido { get; set; }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Iniciando Sistema B...");
            string brokerUri = Environment.GetEnvironmentVariable("ACTIVEMQ_URI") ?? "tcp://localhost:61616";
            IConnectionFactory factory = new ConnectionFactory(brokerUri);
            
            IConnection connection = null;
            
            // Bucle de resiliencia para la conexión
            while (true)
            {
                try
                {
                    Console.WriteLine("Intentando conectar a ActiveMQ...");
                    connection = factory.CreateConnection("admin", "admin");
                    connection.Start();
                    Console.WriteLine("¡Conexión exitosa a ActiveMQ!");
                    break;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[ESPERA] ActiveMQ no está listo: {e.Message}. Reintentando en 5s...");
                    await Task.Delay(5000);
                }
            }
            
            using ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

            IDestination consumeDestination = session.GetQueue("flujo_a_hacia_b");
            IDestination produceDestination = session.GetQueue("flujo_b_hacia_a");

            using IMessageConsumer consumer = session.CreateConsumer(consumeDestination);
            consumer.Listener += new MessageListener(OnMessageReceived);

            using IMessageProducer producer = session.CreateProducer(produceDestination);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

            while (true)
            {
                var payload = new MessagePayload
                {
                    id = $"MSG-{Guid.NewGuid().ToString().Substring(0, 8).ToUpper()}",
                    fecha = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss"),
                    origen = "Sistema-B",
                    destino = "Sistema-A",
                    operacion = "RESPUESTA",
                    contenido = new Contenido { mensaje = "Prueba de integración desde C#" }
                };

                string json = JsonSerializer.Serialize(payload);
                ITextMessage request = session.CreateTextMessage(json);
                
                producer.Send(request);
                Console.WriteLine($"[ENVIADO] ID: {payload.id} hacia {payload.destino}");
                
                await Task.Delay(5000);
            }
        }

        protected static void OnMessageReceived(IMessage message)
        {
            if (message is ITextMessage textMessage)
            {
                try
                {
                    var payload = JsonSerializer.Deserialize<MessagePayload>(textMessage.Text);
                    Console.WriteLine($"[RECIBIDO] ID: {payload.id} | Origen: {payload.origen} | Operación: {payload.operacion}");
                    Console.WriteLine($"[PROCESADO] Mensaje: {payload.contenido.mensaje}");
                }
                catch (Exception)
                {
                    Console.WriteLine($"[ERROR] JSON inválido recibido: {textMessage.Text}");
                }
                
                message.Acknowledge();
            }
        }
    }
}
// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.connector.base.DeliveryGuarantee;
// import org.apache.flink.connector.http.sink.HttpSink;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.api.common.serialization.SimpleStringSchema;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//
// public class KafkaToHttp {
//     public static void main(String[] args) throws Exception {
//         // Ambiente di esecuzione
//         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//         // === CONFIGURAZIONE ===
//         final String KAFKA_BOOTSTRAP = "kafka:29092";
//         final String KAFKA_TOPIC = "mysql.db_cdc.utenti";  // Cambia se necessario
//         final String HTTP_ENDPOINT = "http://host.docker.internal:5000/webhook";
//         final String JOB_NAME = "Debezium-CDC-to-HTTP";
//
//         System.out.println("=== CONFIGURAZIONE JOB ===");
//         System.out.println("Kafka: " + KAFKA_BOOTSTRAP);
//         System.out.println("Topic: " + KAFKA_TOPIC);
//         System.out.println("HTTP Endpoint: " + HTTP_ENDPOINT);
//
//         // 1. SORGENTE KAFKA
//         KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//             .setBootstrapServers(KAFKA_BOOTSTRAP)
//             .setTopics(KAFKA_TOPIC)
//             .setGroupId("flink-cdc-consumer")
//             .setStartingOffsets(OffsetsInitializer.earliest())
//             .setValueOnlyDeserializer(new SimpleStringSchema())
//             .build();
//
//         DataStream<String> kafkaStream = env.fromSource(
//             kafkaSource,
//             WatermarkStrategy.noWatermarks(),
//             "Kafka CDC Source"
//         );
//
//         // 2. TRASFORMAZIONE (Debezium JSON → Payload HTTP)
//         ObjectMapper jsonParser = new ObjectMapper();
//         DataStream<String> httpPayloadStream = kafkaStream
//             .map(event -> {
//                 try {
//                     JsonNode root = jsonParser.readTree(event);
//                     String operation = root.path("op").asText("u"); // 'c'=create, 'u'=update
//                     JsonNode after = root.path("after");
//                     JsonNode before = root.path("before");
//
//                     // Mappa operazioni Debezium
//                     String opType;
//                     switch(operation) {
//                         case "c": opType = "INSERT"; break;
//                         case "u": opType = "UPDATE"; break;
//                         case "d": opType = "DELETE"; break;
//                         case "r": opType = "READ"; break;
//                         default: opType = "UNKNOWN";
//                     }
//
//                     // Costruisci payload semplificato
//                     String payload = jsonParser.createObjectNode()
//                         .put("operation", opType)
//                         .put("timestamp", System.currentTimeMillis())
//                         .put("source", "debezium")
//                         .set("data", after.isMissingNode() ? before : after)
//                         .toString();
//
//                     System.out.println("Processed: " + opType + " -> " + payload.substring(0, Math.min(100, payload.length())) + "...");
//                     return payload;
//
//                 } catch (Exception e) {
//                     System.err.println("ERRORE parsing JSON: " + e.getMessage());
//                     return "{\"error\":\"Parse failed\",\"raw\":\"" +
//                            event.substring(0, Math.min(200, event.length())) + "\"}";
//                 }
//             })
//             .name("JSON Transformer");
//
//         // 3. SINK HTTP
//         HttpSink<String> httpSink = HttpSink.<String>builder()
//             .setEndpointUrl(HTTP_ENDPOINT)
//             .setElementConverter((element, context, request) -> {
//                 request.setHeader("Content-Type", "application/json");
//                 request.setHeader("User-Agent", "Flink-CDC-Job/1.0");
//                 request.setHeader("X-Job-ID", JOB_NAME);
//                 request.setBody(element.getBytes("UTF-8"));
//                 System.out.println("Sending to HTTP: " + element);
//                 return request;
//             })
//             .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)  // Almeno una volta
//             .build();
//
//         // 4. COLLEGAMENTO FINALE
//         httpPayloadStream
//             .sinkTo(httpSink)
//             .name("HTTP Sink");
//
//         // 5. ESEGUZIONE
//         System.out.println("=== AVVIO JOB FLINK ===");
//         env.execute(JOB_NAME);
//     }
// }

/*  */

// import org.apache.flink.api.common.serialization.SimpleStringSchema;
// import org.apache.flink.api.common.typeinfo.TypeHint;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
// import org.apache.flink.types.Row;
// import org.apache.flink.util.Collector;
// import com.getindata.flink.http.connector.HttpSink;
// import com.getindata.flink.http.connector.HttpSinkFunction;
//
// public class KafkaToHttp {
//   public static void main(String[] args) throws Exception {
//     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//     // Kafka source (Debezium)
//     FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//       "mysql.db_cdc.utenti",  // Il tuo topic
//       new SimpleStringSchema(),
//       new java.util.Properties() {{
//         put("bootstrap.servers", "kafka:29092");
//         put("group.id", "flink-http-group");
//       }}
//     );
//
//     DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
//
//     // Processa e invia HTTP
//     kafkaStream.addSink(new HttpSinkFunction<String>() {
//       @Override
//       public void process(String value, Collector<Row> out) {
//         // value è il JSON Debezium
//         // Fai la POST al tuo endpoint sul PC
//         String url = "http://host.docker.internal:3000/webhook";  // Il tuo endpoint
//         // Usa HttpURLConnection o Apache HttpClient per POST (esempio semplice)
//         System.out.println("Invio POST: " + value);
//       }
//     });
//
//     env.execute("Kafka to HTTP Sink");
//   }
// }

/*  */

// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.api.common.serialization.SimpleStringSchema;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import com.getindata.flink.http.connector.HttpSink;
//
// import java.nio.charset.StandardCharsets;
//
// public class KafkaToHttp {
//     public static void main(String[] args) throws Exception {
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//         // 1. Sorgente Kafka
//         KafkaSource<String> source = KafkaSource.<String>builder()
//                 .setBootstrapServers("kafka:29092")
//                 .setTopics("mysql.db_cdc.utenti")
//                 .setGroupId("flink-http-group")
//                 .setStartingOffsets(OffsetsInitializer.earliest())
//                 .setValueOnlyDeserializer(new SimpleStringSchema())
//                 .build();
//
//         DataStream<String> kafkaStream = env.fromSource(
//                 source,
//                 WatermarkStrategy.noWatermarks(),
//                 "Kafka CDC Source"
//         );
//
//         // 2. Sink HTTP (GetInData)
//         HttpSink<String> httpSink = HttpSink.<String>builder()
//                 .setEndpointUrl("http://host.docker.internal:3000/webhook")
//                 .setElementConverter((element, context, request) -> {
//                     // Imposta l'header e il body della richiesta HTTP
//                     request.setHeader("Content-Type", "application/json");
//                     request.setBody(element.getBytes(StandardCharsets.UTF_8));
//                     return request;
//                 })
//                 .build();
//
//         // 3. Collega sorgente e sink
//         kafkaStream.sinkTo(httpSink);
//
//         // 4. Esegui
//         env.execute("Kafka CDC to HTTP via GetInData");
//     }
// }

/*  */

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class KafkaToHttp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("mysql.db_cdc.utenti")
                .setGroupId("flink-http-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka CDC Source"
        );

        DataStream<String> resultStream = kafkaStream
                .map(new HttpSender())
                .setParallelism(1);

        resultStream.print();

        System.out.println("=== AVVIO JOB FLINK (HTTP Sincrono) ===");
        env.execute("Kafka CDC to HTTP (Sincrono)");
    }

    public static class HttpSender extends RichMapFunction<String, String> {
        private transient CloseableHttpClient httpClient;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.httpClient = HttpClients.createDefault();
            this.mapper = new ObjectMapper();
        }

        @Override
        public void close() throws Exception {
            if (httpClient != null) {
                httpClient.close();
            }
        }

        @Override
        public String map(String input) throws Exception {
            try {
                System.out.println("=== RAW KAFKA MESSAGE ===");
                System.out.println(input);

                // Parsing JSON Debezium
                JsonNode root = mapper.readTree(input);

                // Debezium ha questo formato: {"schema":..., "payload":{...}}
                JsonNode payload = root.path("payload");

                if (payload.isMissingNode()) {
                    System.out.println("❌ Nessun campo 'payload' trovato");
                    // Forse è un formato diverso, usa tutto il JSON
                    payload = root;
                }

                JsonNode after = payload.path("after");
                JsonNode before = payload.path("before");
                String op = payload.path("op").asText("u");

                System.out.println("=== PARSED ===");
                System.out.println("op: " + op);
                System.out.println("after: " + after);
                System.out.println("before: " + before);

                // Crea ObjectNode (mutable) invece di usare JsonNode (immutable)
                ObjectNode payloadToSend = mapper.createObjectNode();
                payloadToSend.put("operation", op);
                payloadToSend.put("timestamp", System.currentTimeMillis());

                // Aggiungi i dati appropriati
                JsonNode dataToUse;
                if (op.equals("d")) {
                    // Delete: usa before
                    dataToUse = before.isMissingNode() || before.isNull() ?
                               mapper.createObjectNode() : before;
                } else {
                    // Create/Update/Read: usa after
                    dataToUse = after.isMissingNode() || after.isNull() ?
                               mapper.createObjectNode() : after;
                }

                // Usa set() su ObjectNode, non su JsonNode
                payloadToSend.set("data", dataToUse);

                // Aggiungi anche il payload completo per debug
                payloadToSend.set("full_debezium_payload", payload);

                String finalPayload = payloadToSend.toString();

                System.out.println("=== PAYLOAD TO SEND ===");
                System.out.println(finalPayload);

                // Invia HTTP POST
                HttpPost request = new HttpPost("http://host.docker.internal:3000/events");
                request.setHeader("Content-Type", "application/json");
                request.setHeader("User-Agent", "Flink-HTTP/1.0");
                request.setEntity(new StringEntity(finalPayload, "UTF-8"));

                HttpResponse response = httpClient.execute(request);
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity());

                System.out.println("=== HTTP RESPONSE ===");
                System.out.println("Status: " + statusCode);

                return String.format("HTTP %d: %s", statusCode,
                        input.substring(0, Math.min(100, input.length())));

            } catch (Exception e) {
                System.out.println("=== ERROR ===");
                e.printStackTrace();
                return "ERROR: " + e.getMessage() + " - Input: " +
                       input.substring(0, Math.min(100, input.length()));
            }
        }
    }
}
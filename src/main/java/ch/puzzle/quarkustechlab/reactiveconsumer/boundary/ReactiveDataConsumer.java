package ch.puzzle.quarkustechlab.reactiveconsumer.boundary;

import ch.puzzle.quarkustechlab.reactiveconsumer.control.HeadersMapExtractAdapter;
import ch.puzzle.quarkustechlab.restconsumer.entity.SensorMeasurement;
import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.JsonbBuilder;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

@ApplicationScoped
public class ReactiveDataConsumer {

    private final Logger logger = Logger.getLogger(ReactiveDataConsumer.class.getName());

    @Inject
    Tracer tracer;

    @Incoming("data")
    public CompletionStage<Void> consumeStream(Message<SensorMeasurement> message) {
        logger.info("Message received");
        Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        if (metadata.isPresent()) {
            SpanContext extract = tracer.extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(metadata.get().getHeaders()));
            try (Scope scope = tracer.buildSpan("consume-data").asChildOf(extract).startActive(true)) {
                logger.info("Received jaeger metadata: " + JsonbBuilder.create().toJson(message.getPayload()));
                return message.ack();
            }
        }
        return message.nack(new RuntimeException());
    }
}

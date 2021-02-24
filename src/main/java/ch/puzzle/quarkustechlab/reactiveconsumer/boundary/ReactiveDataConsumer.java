package ch.puzzle.quarkustechlab.reactiveconsumer.boundary;

import ch.puzzle.quarkustechlab.reactiveconsumer.control.HeadersMapExtractAdapter;
import ch.puzzle.quarkustechlab.restconsumer.entity.SensorMeasurement;
import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.JsonbBuilder;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

@ApplicationScoped
public class ReactiveDataConsumer {

    private final Logger logger = Logger.getLogger(ReactiveDataConsumer.class.getName());

    @ConfigProperty(name = "consumer.jaeger.enabled")
    Optional<Boolean> jaegerEnabled;

    @Inject
    Tracer tracer;

    @Incoming("data")
    @Counted(name = "consumedMessages", unit = MetricUnits.NONE, description = "consumed messages by consumer")
    public CompletionStage<Void> consumeStream(Message<SensorMeasurement> message) {

        Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        if (metadata.isPresent()) {
            if(jaegerEnabled.orElse(false)){
                SpanContext extract = tracer.extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(metadata.get().getHeaders()));
                try (Scope scope = tracer.buildSpan("consume-data").asChildOf(extract).startActive(true)) {
                    logger.info("Received reactive message with jaeger metadata: " + JsonbBuilder.create().toJson(message.getPayload()));
                    return message.ack();
                }
            }else{
                logger.info("Received reactive message: " + JsonbBuilder.create().toJson(message.getPayload()));
                return message.ack();
            }
        }
        return message.nack(new RuntimeException());
    }
}

# orders-service

REST and Kafka service generated from the repository skill configuration. Changes are published only after the JPA
transaction commits. Kafka dead-letter topics follow Spring Kafka's default `<topic>.<partition>.DLT` convention;
consumer processing is intentionally a safe extension point. The checked-in reference schema is `Product`, while the
configured domain is `Order`; the local `domain:1.0.0` artifact supplies the configured Order model and contract.

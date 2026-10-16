package transport

import "bridge-kafka-service/internal/schemas"

// PublishChannel carries service responses to be published into their response topic.
var PublishChannel = make(chan *schemas.ConsumerEvent, 1000)

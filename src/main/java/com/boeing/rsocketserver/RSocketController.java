package com.boeing.rsocketserver;

import com.boeing.rsocketserver.data.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;

import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import java.time.Duration;


@Slf4j
@Controller
public class RSocketController {

    static final String SERVER = "Server";
    static final String STREAM = "Stream";
    static final String CHANNEL = "Channel";

    /**
     * When a new request command is received, a new stream of events is started and returned to the client.
     */
    @MessageMapping("stream")
    Flux<Message> stream(final Message request) {
        log.info("Received stream request: {}", request);
        return Flux
                // create a new indexed Flux emitting one element every second
                .interval(Duration.ofSeconds(1))
                // create a Flux of new Messages using the indexed Flux
                .map(index -> new Message(SERVER, STREAM, index));
    }

    /**
     * This @MessageMapping is intended to be used "stream <--> stream" style.
     * The incoming stream contains the interval settings (in seconds) for the outgoing stream of messages.
     */
    @MessageMapping("channel")
    Flux<Message> channel(final Flux<Duration> settings) {
        log.info("Receiving channel request");
        return settings
                .doOnNext(setting -> log.info("Channel frequency setting is {} second(s).", setting.getSeconds()))
                .doOnCancel(() -> log.warn("The client cancelled the channel."))
                .switchMap(setting -> Flux.interval(setting)
                        .map(index -> new Message(SERVER, CHANNEL, index)));
    }
}

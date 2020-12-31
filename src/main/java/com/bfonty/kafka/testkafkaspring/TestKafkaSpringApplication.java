package com.bfonty.kafka.testkafkaspring;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.flogger.Flogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Flogger
public class TestKafkaSpringApplication implements ApplicationRunner {

	@Autowired
	private TwitterConfiguration twitterConfiguration;

	public static void main(String[] args) {
		SpringApplication.run(TestKafkaSpringApplication.class, args);
	}

	@Override
	public void run(final ApplicationArguments args) throws Exception {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = createTwitterClient(msgQueue);
		client.connect();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.atFine().log("stopping application...");
			log.atFine().log("shutting down client from twitter...");
			client.stop();
			log.atFine().log("closing producer...");
			//producer.close();
			log.atFine().log("done!");
		}));

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.atSevere().withCause(e).log("Error");
				client.stop();
			}
			if (msg != null){
				log.atFine().log(msg);
				/*producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if (e != null) {
							logger.error("Something bad happened", e);
						}
					}
				});*/
			}
		}
		log.atFine().log("End of application");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue){

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		hosebirdEndpoint.trackTerms(List.of("switch"));

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(
				twitterConfiguration.getKey(),
				twitterConfiguration.getSecret(),
				twitterConfiguration.getToken(),
				twitterConfiguration.getTokenSecret()
		);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
}

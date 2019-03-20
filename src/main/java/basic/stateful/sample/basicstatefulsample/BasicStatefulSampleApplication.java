package basic.stateful.sample.basicstatefulsample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

@SpringBootApplication
@EnableBinding(KafkaStreamsProcessor.class)
public class BasicStatefulSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(BasicStatefulSampleApplication.class, args);
	}

	@StreamListener("input")
	@SendTo("output")
	public KStream<?, TransactionFailMonitor> process(KStream<?, TransactionStatus> tansactionStream) {

		JsonSerde<TransactionStatus> jsonSerde = new JsonSerde<>(TransactionStatus.class);

		return tansactionStream
				.filter((k,v) -> v.getStatus() == 1 && v.getFailCause() <= 5)
				.map((k, v) -> new KeyValue<>(v.getFailCause(), v))
				.groupByKey(Serialized.with(Serdes.Integer(), jsonSerde))
				.windowedBy(TimeWindows.of(60_000))
				.count(Materialized.as("foo"))
				.toStream()
				.map((k,v) -> new KeyValue<>(null,
						new TransactionFailMonitor(k.key(), v, k.window().start(), k.window().end())));
	}
}

class TransactionStatus {

	private int status; // 0 (approved) or 1 (denied)
	private int failCause;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getFailCause() {
		return failCause;
	}

	public void setFailCause(int failCause) {
		this.failCause = failCause;
	}
}

class TransactionFailMonitor {
	private int failCause;
	private long failCount;
	private long windowStartTime;
	private long windowEndTime;

	public TransactionFailMonitor(int failCause, long failCount, long windowStartTime, long windowEndTime) {
		this.failCause = failCause;
		this.failCount = failCount;
		this.windowStartTime = windowStartTime;
		this.windowEndTime = windowEndTime;
	}

	public int getFailCause() {
		return failCause;
	}

	public void setFailCause(int failCause) {
		this.failCause = failCause;
	}

	public long getWindowEndTime() {
		return windowEndTime;
	}

	public void setWindowEndTime(long windowEndTime) {
		this.windowEndTime = windowEndTime;
	}

	public long getFailCount() {
		return failCount;
	}

	public void setFailCount(long failCount) {
		this.failCount = failCount;
	}

	public long getWindowStartTime() {
		return windowStartTime;
	}

	public void setWindowStartTime(long windowStartTime) {
		this.windowStartTime = windowStartTime;
	}
}

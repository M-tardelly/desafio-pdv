package fiction.brisa27.documents;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DocumentMessaging {
  public static final String QUEUE = "rota-clara.documents";

  @Bean
  Queue documentsQueue() {
    return new Queue(QUEUE, true);
  }
}

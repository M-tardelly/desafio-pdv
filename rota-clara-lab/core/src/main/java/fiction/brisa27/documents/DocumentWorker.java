package fiction.brisa27.documents;

import java.util.List;
import java.util.Map;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DocumentWorker {
  private final DocumentService documents;
  private final RabbitTemplate rabbit;
  private final JdbcTemplate jdbc;
  private final int maxAttempts;

  public DocumentWorker(
      DocumentService documents,
      RabbitTemplate rabbit,
      JdbcTemplate jdbc,
      @Value("${lab.documents.max-attempts:5}") int maxAttempts) {
    this.documents = documents;
    this.rabbit = rabbit;
    this.jdbc = jdbc;
    this.maxAttempts = maxAttempts;
  }

  @RabbitListener(queues = DocumentMessaging.QUEUE)
  public void onMessage(String requestId) {
    documents.process(requestId);
  }

  @Scheduled(fixedDelayString = "${lab.documents.reclaim-delay-ms:5000}")
  public void reclaim() {
    List<Map<String, Object>> rows =
        jdbc.queryForList(
            "SELECT request_id FROM documents WHERE status IN ('PENDING','FAILED','PROCESSING')"
                + " AND attempts < ? AND updated_at < (CURRENT_TIMESTAMP - INTERVAL 3 SECOND)",
            maxAttempts);
    for (var row : rows) {
      rabbit.convertAndSend(DocumentMessaging.QUEUE, (String) row.get("request_id"));
    }
  }
}

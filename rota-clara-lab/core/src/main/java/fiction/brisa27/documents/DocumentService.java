package fiction.brisa27.documents;

import fiction.brisa27.common.BusinessException;
import fiction.brisa27.replenishment.RequestRepository;
import java.net.URI;
import java.util.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Service
public class DocumentService {
  private final RequestRepository repository;
  private final RestClient fiscal;
  private final S3Client s3;
  private final String bucket;
  private final RabbitTemplate rabbit;
  private final int maxAttempts;

  public DocumentService(
      RequestRepository r,
      RabbitTemplate rabbit,
      @Value("${lab.fiscal-url}") String url,
      @Value("${lab.s3-endpoint}") String endpoint,
      @Value("${lab.s3-user}") String user,
      @Value("${lab.s3-password}") String password,
      @Value("${lab.s3-bucket}") String bucket,
      @Value("${lab.documents.max-attempts:5}") int maxAttempts) {
    repository = r;
    this.rabbit = rabbit;
    this.maxAttempts = maxAttempts;
    fiscal = RestClient.builder().baseUrl(url).build();
    this.bucket = bucket;
    s3 =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(user, password)))
            .build();
  }

  @jakarta.annotation.PostConstruct
  public void prepareBucket() {
    for (int attempt = 0; attempt < 40; attempt++) {
      try {
        s3.headBucket(b -> b.bucket(bucket));
        return;
      } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
        if (e.statusCode() == 404) {
          s3.createBucket(b -> b.bucket(bucket));
          return;
        }
      } catch (Exception e) {
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    throw new IllegalStateException("Armazenamento local indisponível no bootstrap");
  }

  public void enqueue(String id) {
    String key = "doc-" + id;
    repository.jdbc().update(
        "INSERT INTO documents(request_id,document_key,status,attempts,reason) VALUES"
            + " (?,?,'PENDING',0,NULL) ON DUPLICATE KEY UPDATE"
            + " status=IF(status IN ('AUTHORIZED_SIMULATION','REJECTED_SIMULATION'), status,"
            + " 'PENDING'), reason=IF(status IN ('AUTHORIZED_SIMULATION','REJECTED_SIMULATION'),"
            + " reason, NULL), updated_at=CURRENT_TIMESTAMP",
        id,
        key);
    Runnable publish = () -> rabbit.convertAndSend(DocumentMessaging.QUEUE, id);
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              publish.run();
            }
          });
    } else {
      publish.run();
    }
  }

  @Transactional
  public void process(String id) {
    var rows =
        repository.jdbc().queryForList(
            "SELECT * FROM documents WHERE request_id=? FOR UPDATE", id);
    if (rows.isEmpty()) return;
    var doc = rows.getFirst();
    String status = (String) doc.get("status");
    if ("AUTHORIZED_SIMULATION".equals(status) || "REJECTED_SIMULATION".equals(status)) return;

    int attempts = ((Number) doc.get("attempts")).intValue();
    if (attempts >= maxAttempts) {
      repository.jdbc().update(
          "UPDATE documents SET status='FAILED',reason=?,updated_at=CURRENT_TIMESTAMP WHERE"
              + " request_id=?",
          "Limite de tentativas documentais atingido",
          id);
      return;
    }

    repository.jdbc().update(
        "UPDATE documents SET status='PROCESSING',attempts=attempts+1,reason=NULL,"
            + " updated_at=CURRENT_TIMESTAMP WHERE request_id=?",
        id);

    String key = (String) doc.get("document_key");
    try {
      Map<?, ?> response = authorizeOrReconcile(id, key);
      if ("REJECTED_SIMULATION".equals(response.get("status"))) {
        repository.jdbc().update(
            "UPDATE documents SET status='REJECTED_SIMULATION',reason=?,protocol=NULL,"
                + " object_key=NULL,updated_at=CURRENT_TIMESTAMP WHERE request_id=?",
            response.get("reason"),
            id);
        return;
      }
      byte[] xml = fiscal.get().uri("/documents/{key}/xml", key).retrieve().body(byte[].class);
      String objectKey = key + ".xml";
      s3.putObject(
          b -> b.bucket(bucket).key(objectKey).contentType("application/xml"),
          RequestBody.fromBytes(xml));
      repository.jdbc().update(
          "UPDATE documents SET status='AUTHORIZED_SIMULATION',protocol=?,object_key=?,"
              + " reason=NULL,updated_at=CURRENT_TIMESTAMP WHERE request_id=?",
          response.get("protocol"),
          objectKey,
          id);
    } catch (Exception e) {
      repository.jdbc().update(
          "UPDATE documents SET status='FAILED',reason=?,updated_at=CURRENT_TIMESTAMP WHERE"
              + " request_id=?",
          "Falha na integração documental: " + e.getClass().getSimpleName(),
          id);
    }
  }

  private Map<?, ?> authorizeOrReconcile(String id, String key) {
    var request = repository.detail(id);
    var items =
        repository.items(id).stream()
            .map(i -> Map.of("product_id", i.get("product_id"), "quantity", i.get("received")))
            .toList();
    var body =
        Map.of(
            "document_key",
            key,
            "delivery_id",
            id,
            "origin_id",
            request.get("origin"),
            "destination_id",
            request.get("destination"),
            "items",
            items);
    try {
      return fiscal.post().uri("/documents").body(body).retrieve().body(Map.class);
    } catch (Exception postError) {
      try {
        return fiscal.get().uri("/documents/{key}", key).retrieve().body(Map.class);
      } catch (Exception ignored) {
        throw postError;
      }
    }
  }

  public byte[] xml(String id) {
    var document = repository.document(id);
    String key = (String) document.get("object_key");
    if (key == null) throw new BusinessException(409, "XML ainda não disponível");
    return s3.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asByteArray();
  }

  public Object mode(String mode) {
    if (!Set.of("sucesso", "rejeicao", "indisponivel", "resposta-perdida").contains(mode))
      throw new BusinessException(400, "Modo desconhecido");
    return fiscal.put().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().body(Map.class);
  }
}

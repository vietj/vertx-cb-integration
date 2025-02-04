package org.example;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import org.junit.*;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class CouchBaseTest {

  private static final String CLUSTER_USERNAME = "Administrator";
  private static final String CLUSTER_PASSWORD = "password";
  private static final DockerImageName COUCHBASE_IMAGE = DockerImageName.parse("couchbase/server").withTag("6.5.1");;
  private static final String BUCKET_NAME = "mybucket";

  private static final CouchbaseContainer SERVER = new CouchbaseContainer(COUCHBASE_IMAGE)
    .withStartupAttempts(5)
    .withStartupTimeout(Duration.ofMinutes(10))
    .withCredentials(CLUSTER_USERNAME, CLUSTER_PASSWORD)
    .withBucket(new BucketDefinition(BUCKET_NAME));

  @BeforeClass
  public static void startCouchbase() {
    SERVER.start();
  }

  @AfterClass
  public static void stopCouchbase() {
    SERVER.stop();
  }

  private Vertx vertx;

  @Before
  public void startVertx() {
    vertx = Vertx.vertx();
  }

  @After
  public void stopVertx() {
    vertx.close();
  }

  @Test
  public void testVanilla() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    ReactiveCluster cluster = ReactiveCluster.connect(
      SERVER.getConnectionString(),
      ClusterOptions.clusterOptions(SERVER.getUsername(), SERVER.getPassword()).environment(env -> {
        // Sets a pre-configured profile called "wan-development" to help avoid
        // latency issues when accessing Capella from a different Wide Area Network
        // or Availability Zone (e.g. your laptop).
        env.applyProfile("wan-development");
      })
    );
    cluster.query("SELECT 2+5 FROM system:dual")
      .flatMapMany(result -> result.rowsAs(JsonObject.class))
      .subscribe(
      result -> {
        System.out.println("Result " + result + " " + Thread.currentThread());
      }, err -> {
        err.printStackTrace();
      }, () -> {
          latch.countDown();
        }
    );
    latch.await(10, TimeUnit.SECONDS);
  }

  @Test
  public void testVertx() throws Exception {
    App app = new App();

    vertx.deployVerticle(app)
      .toCompletionStage()
      .toCompletableFuture()
      .get(10, TimeUnit.SECONDS);

    HttpClient client = vertx.createHttpClient();
    Future<Buffer> resp = client.request(HttpMethod.GET, 8080, "localhost", "/")
      .compose(req -> req
        .send()
        .expecting(HttpResponseExpectation.SC_OK)
        .compose(HttpClientResponse::body));
    Buffer body = resp
      .toCompletionStage()
      .toCompletableFuture()
      .get(10, TimeUnit.SECONDS);
    assertEquals("{\"$1\":7}", body.toString());

    assertNotNull(app.verticleThread);
    assertSame(app.completionThread, app.resultThread);
    assertSame(app.completionThread, app.verticleThread);
  }

  private class App extends AbstractVerticle {

    private ReactiveCluster cluster;
    private HttpServer server;
    volatile Thread resultThread;
    volatile Thread errorThread;
    volatile Thread completionThread;
    volatile Thread verticleThread;

    @Override
    public void start(Promise<Void> startPromise) {
      verticleThread = Thread.currentThread();
      cluster = ReactiveCluster.connect(
        SERVER.getConnectionString(),
        ClusterOptions.clusterOptions(SERVER.getUsername(), SERVER.getPassword()).environment(env -> {
          // Sets a pre-configured profile called "wan-development" to help avoid
          // latency issues when accessing Capella from a different Wide Area Network
          // or Availability Zone (e.g. your laptop).
          env.applyProfile("wan-development");

          // These are the line that matters and integrate the client with Vert.x model
          env.publishOnScheduler(() -> {
            Context context = Vertx.currentContext();
            return Schedulers.fromExecutor(command -> context.runOnContext(event -> command.run()));
        });
        })
      );
      server = vertx.createHttpServer().requestHandler(this::handle);
      server
        .listen(8080, "localhost")
        .<Void>mapEmpty()
        .onComplete(startPromise);
    }

    private void handle(HttpServerRequest request) {
      HttpServerResponse response = request.response().setChunked(true);
      cluster.query("SELECT 2+5 FROM system:dual")
        .flatMapMany(result -> result.rowsAs(JsonObject.class))
        .subscribe(
          result -> {
            resultThread = Thread.currentThread();
            response.write(result.toString());
          }, err -> {
            errorThread = Thread.currentThread();
            response.setStatusCode(500).end();
          }, () -> {
            completionThread = Thread.currentThread();
            response.end();
          }
        );
    }
  }
}

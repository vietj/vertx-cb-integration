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

import static org.junit.Assert.assertEquals;

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
    vertx.deployVerticle(new App())
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
  }

  private class App extends AbstractVerticle {

    private ReactiveCluster cluster;
    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
      cluster = ReactiveCluster.connect(
        SERVER.getConnectionString(),
        ClusterOptions.clusterOptions(SERVER.getUsername(), SERVER.getPassword()).environment(env -> {
          // Sets a pre-configured profile called "wan-development" to help avoid
          // latency issues when accessing Capella from a different Wide Area Network
          // or Availability Zone (e.g. your laptop).
          env.applyProfile("wan-development");
          env.publishOnScheduler(() -> {
            Context context = Vertx.currentContext();
            Scheduler scheduler = Schedulers.fromExecutor(new Executor() {
                @Override
                public void execute(Runnable command) {
                    context.runOnContext(new Handler<Void>() {

                      @Override
                      public void handle(Void event) {
                        command.run();
                      }
                      
                    });
                }
                
            });
            
            return scheduler;
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
        .publishOn(scheduler(vertx.getOrCreateContext()))
        .subscribe(
          result -> {
            System.out.println("Result on: " + Thread.currentThread());
            response.write(result.toString());
          }, err -> {
            System.out.println("Error on : " + Thread.currentThread());
            err.printStackTrace(System.out);
            response.setStatusCode(500).end();
          }, () -> {
            System.out.println("End response on : " + Thread.currentThread());
            response.end();
          }
        );
    }
  }

  /**
   * Scheduler that execute tasks on the vertx context thread (which can be an io event-loop thread, a worker thread
   * or a virtual thread depending on the threading model of the app.
   *
   * This scheduler shall be used for publishing results of a client interaction.
   *
   * This scheduler depends on the current execution thread (hence the {@code context} parameter) and thus cannot
   * be statically set on the client. Hence,  it needs to be created and set for every interaction that publishes
   * a result.
   *
   * A factory like could be used to avoid this, e.g.
   *
   * cluster.setHook(new Supplier<Scheduler>() {
   *   Context current = vertx.getOrCreateContext();
   *   return scheduler(current);
   * });
   *
   * When cluster.query(...).subscribe(...) is called, the hook would be called to get the scheduler to publish
   * on for every call.
   */
  static Scheduler scheduler(Context context) {
    return new Scheduler() {
      @Override
      public Disposable schedule(Runnable r) {
        ContextTask task = new ContextTask(r);
        context.runOnContext(task);
        return task;
      }

      @Override
      public Worker createWorker() {
        return new Worker() {
          @Override
          public Disposable schedule(Runnable r) {
            ContextTask task = new ContextTask(r);
            context.runOnContext(task);
            return task;
          }

          @Override
          public void dispose() {
          }
        };
      }
    };
  }

  static final class ContextTask extends AtomicBoolean implements Handler<Void>, Disposable {

    private final Runnable task;

    ContextTask(Runnable task) {
      this.task = task;
    }

    @Override
    public void handle(Void event) {
      if (!get()) {
        System.out.println("EXECUTING TASK");
        try {
          task.run();
          System.out.println("EXECUTED TASK");
        }
        finally {
          lazySet(true);
        }
      }
    }

    @Override
    public boolean isDisposed() {
      return get();
    }

    @Override
    public void dispose() {
      set(true);
    }
  }
}

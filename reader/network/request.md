

## 数据结构定义

### Request

```scala
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {}
```

#### processor

processor 是 Processor 线程的序号, 标记这个请求是由那个 processor 处理的.
Broker端参数 `num.network.threads` 参数控制了每个Broker监听器上创建的 Processor 数量.

#### context

标志请求上下文信息, 保存了Request所有的请求上下文信息, 
```java
public class RequestContext implements AuthorizableRequestContext {
    public final RequestHeader header;
    public final String connectionId;
    public final InetAddress clientAddress;
    public final KafkaPrincipal principal;
    public final ListenerName listenerName;
    public final SecurityProtocol securityProtocol;
    public final ClientInformation clientInformation;
    public final boolean fromPrivilegedListener;
    public final Optional<KafkaPrincipalSerde> principalSerde;
//    ...
}
```

#### startTimeNanos


#### memoryPool

#### buffer


#### metrics

#### envelope



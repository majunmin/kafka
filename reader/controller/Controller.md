# 


## Kafka Controller




### Controller 管理请求的发送


Controller 事件管理组件:

- QueuedEvent: 事件队列中的事件对象.
- ControllerEventProcessor: KafkaController 是 ControllerEventProcessor 接口的唯一实现类, 
- ControllerEventThread: 消费者类, 消费 QueuedEvent 事件.
- ControllerEventManager: 创建和管理事件处理线程和事件的队列, 定义了 ControllerEventThread 类. 


![controller管理请求的发送.png](controller%E7%AE%A1%E7%90%86%E8%AF%B7%E6%B1%82%E7%9A%84%E5%8F%91%E9%80%81.png)


## Controller leader 选举


### controller 选举触发场景.

![controller选举触发场景.png](controller%E9%80%89%E4%B8%BE%E8%A7%A6%E5%8F%91%E5%9C%BA%E6%99%AF.png)

#### 1. controller 从零启动

集群启动时, controller 节点尚未被选举出来, broker 启动后 首先将 `Startup` 这个 ControllerEvent 写入事件队列,
启动对应的事件处理线程和 `ControllerChangeHandler` Zookeeper监听器, 最后依赖事件处理线程进行 leader选举.

```scala
  /**
 * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
 * is the controller. It merely registers the session expiration listener and starts the controller leader
 * elector
 */
def startup() = {
  // 注册Zookeeper状态变更监听器,用于监听zookeeper 会话过期的.
  zkClient.registerStateChangeHandler(new StateChangeHandler {
    override val name: String = StateChangeHandlers.ControllerHandler
    override def afterInitializingSession(): Unit = {
      eventManager.put(RegisterBrokerAndReelect)
    }
    override def beforeInitializingSession(): Unit = {
      val queuedEvent = eventManager.clearAndPut(Expire)

      // Block initialization of the new session until the expiration event is being handled,
      // which ensures that all pending events have been processed before creating the new session
      queuedEvent.awaitProcessing()
    }
  })
  // 2. 将 Startup事件添加到事件队列.
  eventManager.put(Startup)
  // 3. 启动事件处理线程, 开始处理事件队列中的 controllerEvent.
  eventManager.start()
}
```



KafkaController.processStartup() 处理 startup事件：
```scala


  private def processStartup(): Unit = {
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    elect()
  }

```

#### 2. controller 节点消失

#### 3. controller 节点数据发生变更
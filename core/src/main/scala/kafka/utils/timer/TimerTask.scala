/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

trait TimerTask extends Runnable {

  val delayMs: Long // timestamp in millisecond[request.timeout.ms参数值]

  // 每个TimerTask 关联一个TimerTaskEntry,
  // 也就是说 每个TimerTask 都需要知道他在哪个Bucket 链表下的哪个链表元素上.
  private[this] var timerTaskEntry: TimerTaskEntry = null

  // 取消延时任务, 原理就是将对应的 timerTaskEntry 设置为null
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  // 关联TimerTask,就是将TimerTask 设置到 TimerTaskEntry 上.
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }

  // 获取对应的 TimerTaskEntry实例.
  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}

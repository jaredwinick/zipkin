package com.twitter.zipkin.util

import com.twitter.util.FuturePool
import java.util.concurrent.Executors

// Copied directly from AnormThreads
object AccumuloThreads {

  // Cached pools automatically close threads after 60 seconds
  private val threadPool = Executors.newCachedThreadPool()

  /**
   * Execute a callback in a separate thread.
   */
  def inNewThread = FuturePool(threadPool)

}
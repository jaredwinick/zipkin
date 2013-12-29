package com.twitter.zipkin.accumulo

import com.twitter.zipkin.storage.accumulo.AccumuloStorage
import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.Storage

object StorageBuilder {
  def apply(
    instanceName: String, 
    zooKeepers: String,
    user: String,
    password: String) = {
    	new StorageBuilder(instanceName, zooKeepers, user, password)
  }
}
class StorageBuilder(
    instanceName: String, 
    zooKeepers: String,
    user: String,
    password: String) extends Builder[Storage] {
  
	def apply() = {
	  AccumuloStorage(instanceName, zooKeepers, user, password)
	}
}
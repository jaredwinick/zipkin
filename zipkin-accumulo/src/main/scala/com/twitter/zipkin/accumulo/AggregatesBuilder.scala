package com.twitter.zipkin.accumulo

import com.twitter.zipkin.builder.Builder

object AggregatesBuilder {
	def apply() = {
	  new AggregatesBuilder()
	}
}

class AggregatesBuilder() extends Builder[Aggregates] {
 
}
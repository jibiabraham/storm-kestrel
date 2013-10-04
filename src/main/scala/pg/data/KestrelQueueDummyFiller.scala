package com.pg.kestrelqueuedummyfiller

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.{Client, ReadHandle}
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.kestrel.protocol._

import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import com.twitter.util.JavaTimer
import com.twitter.conversions.time._
import com.twitter.util.{Time, Future}

object KestrelQueueDummyFiller {
  /*
    The Kestrel Client constructor has a really handy apply which basically does
      val serviceFactory = ClientBuilder()
        .hosts("localhost:22133")
        .codec(Kestrel())
        .hostConnectionLimit(1)
        .buildFactory()
      val client = Client(serviceFactory)

      for us.
   */

  def main(args: Array[String]): Unit = {
    // Add "host:port" pairs as needed
    val host = "localhost:22133"

    val client = Client(ClientBuilder()
      .codec(Kestrel())
      .hosts(host)
      .hostConnectionLimit(10)
      .buildFactory())

    val queueName = "spam"

    def insertIntoQueue(max_inserts: Int, collector: Int = 0): Future[Response] = {
      client.set("spam", copiedBuffer("abc" + collector, CharsetUtil.UTF_8)) onSuccess { r => 
        Console.println("Inserted abc" + collector)
        if (collector < max_inserts){
          insertIntoQueue(max_inserts, collector + 1)
        }
      }
    }

    insertIntoQueue(100);

  }

}
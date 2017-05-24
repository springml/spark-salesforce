//package com.springml.spark.salesforce
//
//import scala.concurrent.{ Future, Promise }
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.annotation.tailrec
//import scala.util.{ Success, Failure }
//import scala.concurrent._
//import scala.concurrent.duration.Duration
//import ExecutionContext.Implicits.global
//import scala.util.control.NonFatal
//import scala.concurrent.duration._
//import com.springml.salesforce.wave.model.BatchInfo
//import com.springml.salesforce.wave.model.BatchResult
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.util.Random
//import scala.Left
//import scala.Right
//import java.io.InputStream
//
//private class BatchProcessor {
//
//  /**
//   * Retry the function call n number of times, with exponentially increasing pauses in between non-fatal failures
//   */
//  @annotation.tailrec
//  final def retry[T](n: Int, pause: Duration)(fn: => T): T = {
//    scala.util.Try { fn } match {
//      case Success(x) => x
//      case Failure(e) if n > 1 && NonFatal(e) => {
//        println(s"${e.getMessage} - Pausing ${pause.toMillis} to retry")
//        Thread.sleep(pause.toMillis)
//        retry(n - 1, pause + pause)(fn)
//      }
//      case Failure(e) => { println(s"${e.getMessage}. Will NOT retry"); throw e }
//    }
//  }
//
//  /**
//   * Completes upon all Futures complete or upon first failure.
//   * Sets the sample result from the first success
//   */
//  @tailrec 
//  final def awaitSuccess[BatchResult](
//    futureSeq: Seq[Future[BatchResult]],
//    done: Seq[BatchResult] = Seq()): Either[Throwable, Seq[BatchResult]] = {
//    val first = Future.firstCompletedOf(futureSeq)
//
//    Await.ready(first, Duration.Inf).value match {
//      case None             => awaitSuccess(futureSeq, done) // Shouldn't happen!
//      case Some(Failure(e)) => Left(e)
//      case Some(Success(_)) =>
//        if (!batchResult.isDefined) {
//          println(s"FirstSuccess:${first.value}")
//          batchResultPromise completeWith first.asInstanceOf[scala.concurrent.Future[com.springml.salesforce.wave.model.BatchResult]]
//        }
//        val (complete, running) = futureSeq.partition(_.isCompleted)
//        val answers = complete.flatMap(_.value)
//        answers.find(_.isFailure) match {
//          case Some(Failure(e)) => Left(e)
//          case _ =>
//            if (running.length > 0) awaitSuccess(running, answers.map(_.get) ++: done)
//            else Right(answers.map(_.get) ++: done)
//        }
//    }
//  }
//
//  val batchResultPromise = Promise[BatchResult]()
//  val promiseFuture = batchResultPromise.future
//  promiseFuture onSuccess {
//    case result =>
//      { println("HEREEEEEEEEEEEEE"); batchResult = Some(result); batchResult; }
//  }
//
//  var batchResult: Option[BatchResult] = None
//
//  def sample = {
//    awaitSuccess(batchProcessors)
//    batchResult
//  }
//
//  def processBatch(bi: BatchInfo): BatchResult = {
//    println(s"starting to process ${bi.getId}")
//    Thread.sleep(500 + Random.nextInt(3000))
//    val r = Random.nextInt(5) % 2
//    if (r == 0) throw new RuntimeException(s"Failure while processing ${bi.getId}")
//    println(s"${bi.getId} finished")
//    new BatchResult(bi.getJobId, bi.getId, "file://${bi.getId}");
//  }
//
//  def batchProcessors: Seq[Future[BatchResult]] = {
//    //    (1 to 5) map { i => val bi = new BatchInfo(); bi.setId(i.toString()); Future { processBatch(bi) } }
//    (1 to 5) map { i => val bi = new BatchInfo(); bi.setId(i.toString()); Future { retry(3, 1 second) { processBatch(bi) } } }
//  }
//
//  def processBatches(batches: Seq[BatchInfo])(fn: BatchInfo => BatchResult) = {
//    batches.map(bi => Future { retry(3, 1 second) { processBatch(bi) } })
//  }
//
//  def main(args: Array[String]): Unit = {
//    //    promiseFuture onSuccess {
//    //      case a: BatchResult => println(s"sample calculated! $batchResult $a")
//    //      case _              => println("what the heck!")
//    //    }
//    // val ff = Future { BatchResult("999") }
//
//    // batchResultPromise success BatchResult("888")
//    //    ff.value
//    println(s"initial sample ! $batchResult, sample")
//    awaitSuccess(batchProcessors)
//    println(s"final sample ! $batchResult, $sample")
//  }
//}
//
//private[salesforce] object DefaultBatchProcessor extends BatchProcessor
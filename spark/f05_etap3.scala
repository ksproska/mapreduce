import scala.concurrent.duration._

val before = System.currentTimeMillis;

// code

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
System.exit(0)
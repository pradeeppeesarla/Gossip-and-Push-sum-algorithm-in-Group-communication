import akka.actor._
import scala.util.Random
import Array._
import akka.actor.Scheduler
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object project2bonus {
  
  case class Start(actors: Array[ActorRef], neighbours: List[Int], sval: Double, listener: ActorRef)
  case class doGossip(message: String, index: Int)
  case class doPushSum(sval: Double, wval: Double, index: Int)
  case class gossipConverge(index: Int)
  case class pushSumConverge(sval: Double, wval: Double, index: Int)
  
  def main(args: Array[String]){
    if(args.length != 4){
      println("Invalid Arguments")
    }
    else{
      println("Algorithm : " + args(2))
      println("Topology : " + args(1))
      println("Nodes : " + args(0))
      var numNodes = args(0).toInt
      var num2D: Double = 0
      if(args(1).equalsIgnoreCase("2D") || args(1).equalsIgnoreCase("Imp2D")){
        num2D = Math.sqrt(numNodes)
        while(num2D != num2D.toInt){
          numNodes = numNodes + 1
          num2D = Math.sqrt(numNodes)
        }
      }
      var i = 0
      var failureNodes: List[Int] = Nil
      while(i < args(3).toInt){
        var j = Random.nextInt(numNodes)
        while(failureNodes.contains(j)){
          j = Random.nextInt(numNodes)
        }
        //println(i)
        failureNodes ::= j
        i = i + 1
      }
      i = 0
      //println("Done with failure nodes")
      var actors = new Array[ActorRef](numNodes)
      val system = ActorSystem("DosSystem")
      while(i < numNodes){
        actors(i) = system.actorOf(Props(new Worker()), name = "Worker"+i) 
        i = i + 1
      }
      //println("Done with actors")
      i = 0
      val b = System.currentTimeMillis;
      val listener = system.actorOf(Props(new Listener(numNodes, b, args(3).toInt, args(2))))
      var neighbours2: List[Int] = Nil
      if(args(1).equalsIgnoreCase("full")){
          //println("Entered full section")
          var k: Int = 0
          //println(numNodes)
          while(k < numNodes){
            neighbours2 ::= k
            k = k + 1
          }
          //println("Done with while")
      }
      while(i < numNodes){
        var neighbours: List[Int] = Nil
        if(failureNodes.contains(i)){
          actors(i) = null
          i = i + 1
        }
        else{
          if(args(1).equalsIgnoreCase("full")){
            //println("Entered full section")
            //println(numNodes)
            //println("Done with while")
          }
          else if(args(1).equalsIgnoreCase("line")){
            if(i != 0){
              neighbours ::= i - 1
            }
            if(i != numNodes - 1){
              neighbours ::= i + 1
            }
          }
          else if(args(1).equalsIgnoreCase("2D") || args(1).equalsIgnoreCase("Imp2D")){
            var k: Int = Math.sqrt(numNodes).toInt
            if(i % k != 0 && i > k && i < numNodes - k && (i +1) % k != 0){
              neighbours ::= i - 1
              neighbours ::= i + 1
              neighbours ::= i - k
              neighbours ::= i + k
            }
            else if(i % k == 0 ){
              neighbours ::= i + 1
              if(i + k < numNodes){
                neighbours ::= i + k
              }
              if(i - k >= 0){
                neighbours ::= i - k
              }
            }
            else if((i + 1) % k == 0){
              neighbours ::= i - 1
              if(i + k < numNodes){
                neighbours ::= i + k
              }
              if(i - k >= 0){
                neighbours ::= i - k
              }
            }
            else if(i > 0 && i < k - 1){
              neighbours ::= i + 1
              neighbours ::= i - 1
              neighbours ::= i + k
            }
            else{
              neighbours ::= i + 1
              neighbours ::= i - 1
              neighbours ::= i - k
            }
          }
          if(args(1).equalsIgnoreCase("Imp2D")){
            var k: Int = i
            while(k == i || neighbours.contains(k)){
              k = Random.nextInt(numNodes)
            }
            neighbours ::= k
          }
          if(args(1).equalsIgnoreCase("full"))
            actors(i) ! Start(actors, neighbours2, i + 1, listener)
          else
            actors(i) ! Start(actors, neighbours, i + 1, listener)
          i = i + 1
        }
      }
      if(args(2).equalsIgnoreCase("gossip")){
        //println("Hello2")
        var rand: Int = Random.nextInt(numNodes)
        while(failureNodes.contains(rand)){
          rand = Random.nextInt(numNodes)
        }
        //println("Hello")
        actors(rand) ! doGossip("Gossip Message", rand)
      }
      else if(args(2).equalsIgnoreCase("push-sum")){
        var rand: Int = 0
        while(failureNodes.contains(rand) && rand < numNodes){
          rand = rand + 1
        }
        if(rand < numNodes){
          actors(rand) ! doPushSum(rand, 1, rand)
        }
        else{
          println("Couldn't find a node to start push-sum")
        }
      }  
    }
  }
  
  class Listener(numNodes: Int, b: Long, numFailure: Int, algo: String) extends Actor{
    var count = 0
    var s: Double = 0
    var w: Double = 0
    var recentB: Long = 0
    var scheduleFlag: Int = 1
    
    def receive = {
      
      case gossipConverge(index) =>
        //println("Converged")
        scheduleFlag = 1
        if(count == 0){
          context.system.scheduler.scheduleOnce(10000 milliseconds, self, "schedule")
          scheduleFlag = 0
        }
        count = count + 1
        //println("Counted : " + count)
        if(count == numNodes - numFailure){
          println("Time taken to converge : " + (System.currentTimeMillis() - b))
          System.exit(0)
        }
        
      case pushSumConverge(sval, wval, index) =>
        scheduleFlag = 1
        if(count == 0){
          context.system.scheduler.scheduleOnce(10000 milliseconds, self, "schedule")
          scheduleFlag = 0
        }
        count = count + 1
        s = s + sval
        w = w + wval
        if(count == (numNodes - numFailure)){
          println("Time taken for 60% of the nodes to converge : " + (System.currentTimeMillis() - b))
          println("Average ratio of all the 60% of the nodes : " + count)
          System.exit(0)
        }
        
      case "schedule" =>
        if(scheduleFlag == 0){
          println("Time taken for " + count + " nodes to converge : " + (System.currentTimeMillis() - b - 10000))
          if(algo.equalsIgnoreCase("push-sum"))
            println("Average ratio of all the nodes converged : " + (s/w))
          System.exit(0)
        }
        else{
          context.system.scheduler.scheduleOnce(10000 milliseconds, self, "schedule")
          scheduleFlag = 0
        }
      
    }
    
  }
  
  class Worker extends Actor{
    
    var actors: Array[ActorRef] = null
    var neighbours: List[Int] = null
    var s: Double = 0
    var w: Double = 0
    var listener: ActorRef = null
    var count: Int = 0
    var scheduleFlag: Int = 1
    var prev: Double = 0
    var pushCount: Int = 0
    var status = true
    var threshold = 0
    var permIndex = -1
    
    def receive = {
      
      case Start(actors, neighbours, sval, listener) =>
        this.actors = actors
        this.neighbours = neighbours
        this.s = sval
        this.w = 1
        this.listener = listener
        this.threshold = actors.length*2/100
        this.permIndex = sval.toInt - 1
        //println("Started")
      
      case doGossip(message, index) =>
        if(index != -1){
          count = count + 1
          scheduleFlag = 1
        }
        if(count == 1){
          listener ! gossipConverge(index)
        }
        if(status){
          context.system.scheduler.scheduleOnce(50 milliseconds, self, doGossip(message, -1))
          scheduleFlag = 0
        }
        if(count == threshold && index != -1){
          //println("Actor: " + index + " stopped transmitting")
          status = false
        }
        else if(count < threshold){
          var rand: Int = neighbours(Random.nextInt(neighbours.length))
          var whileCount = 0
          while(actors(rand) == null || rand == permIndex){
            rand = neighbours(Random.nextInt(neighbours.length))
            whileCount = whileCount + 1
            if(whileCount > actors.length * 4){
              status = false
              println("Actor: " + index + " stopped transmitting, no neighbours found")
              count = threshold + 2
            }
          }
          if(status)
            actors(rand) ! doGossip(message, rand)
        }
        
      case doPushSum(sval, wval, index) =>
        if(index != -1){
          count = count + 1
          scheduleFlag = 1
        }
        if(status){
          context.system.scheduler.scheduleOnce(50 milliseconds, self, doPushSum(s, w, -1))
          scheduleFlag = 0
        }
        this.s = (this.s + sval)/2
        this.w = (this.w + wval)/2
        if(count > 3 && index != -1){
          if(prev - (this.s/this.w) < Math.pow(10, -10)){
            pushCount = pushCount + 1
          }
          else{
            pushCount = 0
          }
        }
        if(pushCount == 3 && status){
          status = false
          listener ! pushSumConverge(this.s, this.w, index)
        }
        if(status){
          var rand: Int = neighbours(Random.nextInt(neighbours.length))
          var whileCount = 0
          while(actors(rand) == null || rand == permIndex){
            rand = neighbours(Random.nextInt(neighbours.length))
            whileCount = whileCount + 1
            if(whileCount > actors.length * 4){
              status = false
              //println("Actor: " + index + " stopped transmitting, no neighbours found")
              listener ! pushSumConverge(this.s, this.w, index)
              count = threshold + 2
            }
          }
          if(status)
            actors(rand) ! doPushSum(this.s, this.w, rand)
        }
    }
    
  }
  
}
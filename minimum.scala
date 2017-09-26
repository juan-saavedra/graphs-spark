import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices: RDD[(VertexId, Int)] = sc.parallelize(
  Array((1L, 3), (2L, 6), (3L, 2), (4L, 1), (5L, 10))
)

val relationships: RDD[Edge[Boolean]] = sc.parallelize(Array(
    Edge (1L, 2L, true ), Edge (2L, 1L, true ), Edge (2L, 4L, true ),
    Edge (3L, 2L, true ), Edge (3L, 4L, true ), Edge (4L, 3L, true ),
    Edge (1L, 5L, true ))
)

val graph = Graph(vertices, relationships).mapVertices((id, attr) => (attr, Int.MaxValue))

val initialMsg = 9999
def vprog(vertexId: VertexId, value: (Int, Int), message: Int): (Int, Int) = {
  if (message == initialMsg) value else (math.min(message, value._1), value._1)
}

def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
  val sourceVertex = triplet.srcAttr
  if (sourceVertex._1 == sourceVertex._2) {
    Iterator.empty
  }
  else {
    Iterator((triplet.dstId, sourceVertex._1))
  }
}

def mergeMsg(msg1: Int, msg2: Int): Int = math.min(msg1, msg2)

val minGraph = graph.pregel(initialMsg, Int.MaxValue, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)

for (triplet <- minGraph.triplets.collect) {
  println(s"[id: ${triplet.srcId}, data: ${triplet.srcAttr}] -> " +
  s"${triplet.attr} -> [id: ${triplet.dstId}, data: ${triplet.dstAttr}]")
}
println(s"Minimum value is " + minGraph.vertices.take(1)(0)._2._1)

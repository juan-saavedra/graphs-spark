import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertexArray = Array(
  (1L, ("Santiago")), (2L, ("Concepción")), (3L, ("Temuco")), (4L, ("Pucón")), (5L, ("Osorno"))
)

val edgeArray = Array(
  Edge(1L, 2L, 500), Edge(1L, 3L, 680), Edge(2L, 3L, 300), Edge(3L, 4L, 105),
  Edge(3L, 5L, 250), Edge(4L, 5L, 240)
)

val vertices: RDD[(VertexId, String)] = sc.parallelize(vertexArray)
val relationships: RDD[Edge[Int]] = sc.parallelize(edgeArray)
val graph = Graph(vertices, relationships).mapVertices((id, attr) => {
  if(id == 1) (attr, 0.0) else (attr, Double.MaxValue)
})

val initialMsg = Double.MaxValue
def vprog(id: VertexId, attr: (String, Double), newDistance: Double):
         (String, Double) = {
  if (newDistance == initialMsg) {
    attr
  }
  else {
    (attr._1, math.min(newDistance, attr._2))
  }
}

def sendMsg(triplet: EdgeTriplet[(String, Double), Int]):
            Iterator[(VertexId, Double)] = {
  val sourceVertex = triplet.srcAttr
  val edge = triplet.attr
  if (sourceVertex._2 == Double.MaxValue) {
    Iterator.empty
  }
  else {
    Iterator((triplet.dstId, sourceVertex._2 + edge))
  }
}

def mergeMsg(a: Double, b: Double): Double = math.min(a, b)

val shortestGraph = graph.pregel(initialMsg, Int.MaxValue, EdgeDirection.Out)(vprog,
                                 sendMsg, mergeMsg)
shortestGraph.vertices.collect.foreach(v => println(s"${v}"))

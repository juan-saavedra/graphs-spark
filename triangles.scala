import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertexArray = Array(
  (1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 5), (6L, 6), (7L, 7)
)

val edgeArray = Array(
  Edge(1L, 2L, true), Edge(2L, 3L, true), Edge(3L, 1L, true), Edge(3L, 4L, true),
  Edge(4L, 5L, true), Edge(5L, 6L, true), Edge(6L, 4L, true), Edge(7L, 3L, true)
)

val vertices : RDD [( VertexId , Int )] = sc.parallelize(vertexArray)
val relationships : RDD [ Edge [ Boolean ]] = sc.parallelize(edgeArray)
val graph = Graph(vertices, relationships).mapVertices((id, attr) => {
  (attr, List[(Int, Int, Int)](), 0 )
})

def updatePath(path: (Int, Int, Int), id: Int, iteration: Int): (Int, Int, Int) = {
  iteration match {
    case 0 => path.copy(_1 = id)
    case 1 => path.copy(_2 = id)
    case 2 => path.copy(_3 = id)
    case _ => path
  }
}

val initialMsg = (List((-1, -1, -1)), 0)
def vprog(vertexId: VertexId, attr: (Int, List[(Int, Int, Int)], Int),
          newPath: (List[(Int, Int, Int)], Int)): (Int, List[(Int, Int, Int)], Int) = {
  var pathList = List[(Int, Int, Int)]()
  for (path <- newPath._1) {
    pathList = pathList :+ updatePath(path, attr._1, newPath._2)
  }
  (attr._1, pathList, newPath._2 + 1)
}

def sendMsg(triplet: EdgeTriplet[(Int, List[(Int, Int, Int)], Int), Boolean]):
            Iterator[(VertexId, (List[(Int, Int, Int)], Int))] = {
  val sourceVertex = triplet.srcAttr
  if (sourceVertex._3 >= 4) {
    Iterator.empty
  }
  else {
    Iterator((triplet.dstId, (sourceVertex._2, sourceVertex._3)))
  }
}

def mergeMsg(a: (List[(Int, Int, Int)], Int), b: (List[(Int, Int, Int)], Int)):
            (List[(Int, Int, Int)], Int) = (a._1 ++ b._1, a._2)

val triGraph = graph.pregel(initialMsg, 10, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
val totalTriangles = triGraph.vertices.map(vertex => {
  val vertexData = vertex._2
  var id = vertexData._1
  var pathList = vertexData._2
  val filteredPaths = pathList.filter(path => !path.productIterator.toList.contains(-1) && path._1 == id)
  filteredPaths.size
}).reduce((a, b) => a + b) / 3
println("Total de triángulos es " + totalTriangles)
triGraph.vertices.collect.foreach(v => println(s"${v}"))



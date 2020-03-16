package utilities

object FilesPath {
    private var tempPathMatrix: Array[Array[String]] = Array()

    def initFilePathMatrix(rows: Int, cols: Int): Unit = {
      tempPathMatrix =Array.ofDim[String](rows, cols)
      for {
        row <- 0 until rows
        col <- 0 until cols
      } tempPathMatrix(row)(col) = ""

    }

    def getMatrixFilePath(row: Int, col: Int): String = {
      tempPathMatrix(row)(col)
    }

    def setMatrixFilePath(row: Int, col: Int, path: String): Unit = {
      tempPathMatrix(row)(col) = path
    }

    def getPathMatrix(): Array[Array[String]] = {
      tempPathMatrix
    }

}

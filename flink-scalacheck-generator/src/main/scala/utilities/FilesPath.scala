package utilities

/** Object that stores paths of data generated in each attempt */
object FilesPath {
    private var tempPathMatrix: Array[Array[String]] = Array()

  /**
   * Init matrix which will store the path of every file regarding the attempts and partitions
   * @param rows rows
   * @param cols cols
   */
    def initFilePathMatrix(rows: Int, cols: Int): Unit = {
      tempPathMatrix =Array.ofDim[String](rows, cols)
      for {
        row <- 0 until rows
        col <- 0 until cols
      } tempPathMatrix(row)(col) = ""

    }

  /**
   * Get a specific position of the matrix
   * @param row row
   * @param col col
   * @return
   */
    def getMatrixFilePath(row: Int, col: Int): String = {
      tempPathMatrix(row)(col)
    }

  /**
   * Set a specific path for a given row and col in the matrix
   * @param row row
   * @param col col
   * @param path path
   */
    def setMatrixFilePath(row: Int, col: Int, path: String): Unit = {
      tempPathMatrix(row)(col) = path
    }

  /**
   * Get matrix
   * @return matrix
   */
  def getPathMatrix(): Array[Array[String]] = {
      tempPathMatrix
    }

}

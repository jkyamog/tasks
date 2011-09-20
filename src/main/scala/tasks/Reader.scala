package tasks

import scala.io.Source
import scala.collection
import collection.mutable.ArrayBuffer

object Reader {
	
	def readFile(fileName: String) = {
		val lines = Source.fromFile(fileName, "UTF-8").getLines()
		val splittedLines = for (line <- lines) yield line.split(",") 
		splittedLines.toList
	}

	def processCell(header: String, cell: String): String = {
		processors.get(header) match {
			case Some(processor) => processor.first._2(cell)
			case None => cell
		}
	}
	
	def processData(headers: Array[String], rows: List[Array[String]]) = {
		
		val newHeaders: IndexedSeq[String] = headers map { header =>
			processors.get(header) match {
				case Some(processor) => processor.first._1
				case None => header
			}
		}
		
		val newRows = rows.par map { row => 
			val columnNos = 0 until row.size
			columnNos map { colNo =>
				processCell(headers(colNo), row(colNo))
			}
		}
		newHeaders :: newRows.toList
	}

	
	val processors = Map (
			"col1" -> Map("Col1" -> complicated1),
			"col2" -> Map("Column2" -> complicated2),
			"col4" -> Map("4" -> (complicated3 andThen complicated1))
			)
	
	lazy val noop = (input: String) => input
	lazy val complicated1 = (input: String) => {
		(BigDecimal(input) * Math.pow(10, 20) / 33) toString
	}
	lazy val complicated2 = (input: String) => {
		try {
			(BigDecimal(input) * Math.pow(input.toDouble, 50) / 31) toString
		} catch {
			case _ => input
		}
	}
	lazy val complicated3 = (input: String) => {
		10 * input.toDouble toString
	}

	def main(args: Array[String]) {

		readFile(Config.csvfile).toList match {
			case headers :: rows => {
				val start = System.currentTimeMillis
				val result = processData(headers, rows)
				val finish = System.currentTimeMillis
				result foreach println
				println(finish - start)
			}
			case _ => println("No data, expecting a header and a data")
		}
	}

}


object Config {
	val csvfile = "/Users/jyamog/tmp/test.csv" 
}

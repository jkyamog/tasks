package tasks

import scala.io.Source
import scala.collection
import collection.mutable.ArrayBuffer

object Reader {
	
	def readFile(fileName: String) = {
		val lines = Source.fromFile(fileName, "UTF-8").getLines()
		val splittedLines = for (line <- lines) yield line.split(",") 
		splittedLines
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
			for (colNo <- 0 until row.size)
				yield processCell(headers(colNo), row(colNo))
		}
		newHeaders :: newRows.toList
	}

	
	val processors = Map (
			"col1" -> Map("Col1" -> complicated1),
			"col2" -> Map("Column2" -> complicated2),
			"col3" -> Map("Col3" -> complicated1),
			"col4" -> Map("4" -> (complicated3 andThen complicated1)),
			"col5" -> Map("five" -> complicated2),
			"col6" -> Map("s-i-x" -> complicated2)
			)
	
	lazy val noop = (input: String) => input
	lazy val complicated1 = (input: String) => {
		(BigDecimal(input) * 30000000000l / 333333333l) toString
	}
	lazy val complicated2 = (input: String) => {
		try {
			(BigDecimal(input) / 20000000l * Math.pow(input.toDouble, 20)) toString
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
				for (i <- 1 to 4) processData(headers, rows)
				
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

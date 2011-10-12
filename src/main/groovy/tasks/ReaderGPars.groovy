package tasks

import groovyx.gpars.GParsPool
import groovyx.gpars.ParallelEnhancer

class ReaderGPars {
	
	def readFile(path) {
		def result = [][]
		new File(path).withReader{ reader ->
			reader.eachLine{ line ->
				result += line.split(",")
			}
		}
	}

	def processCell(header, cell) {
		if (processors.containsKey(header)) {
			def func = processors.get(header) find { true } value
			func(cell)
		} else {
			cell
		}
	}

	def processData(headers, rows) {
		def newHeaders = headers collect { header ->
			if (processors.containsKey(header)) {
				processors.get(header) find { true } key
			} else {
				header
			}
		}
		
		ParallelEnhancer.enhanceInstance(rows)
		def newRows = rows.collectParallel { row ->
			def newRow = []
			row.eachWithIndex { column, colIndex ->
				newRow += processCell(headers[colIndex], column)
			}
			[newRow]
		}

		return [newHeaders] + newRows
	}
	
	def complicated1 = { input ->
		(input as BigDecimal) * 30000000000l / 333333333l
	}
	
	def complicated2 = { input ->
		try {
			(input as BigDecimal) / 20000000l * Math.pow(input as Double, 20)
		} catch (e) {
			input
		}
	}
	
	def complicated3 = { input -> 
		10 * (input as Double)
	}

	
	def processors = [
			"col1": ["Col1": complicated1],
			"col2": ["Column2": complicated2],
			"col3": ["Col3": complicated1],
			"col4": ["4": complicated3 << complicated1],
			"col5": ["five": complicated2],
			"col6": ["s-i-x": complicated2],
		]	
	
	static main(args) {
		def rG = new ReaderGPars()
		
		def result = rG.readFile(Config.csvfile)
		def headers = result[0]
		def rows = result[1..(result.size - 1)]
		
		for (i in 1..4) rG.processData(headers, rows)

		def start = System.currentTimeMillis()
		def processedData = rG.processData(headers, rows)	
		def finish = System.currentTimeMillis()
		
		processedData.each { row ->
			println(row)	
		}
		println(finish - start)
		
	}

}


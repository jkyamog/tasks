package tasks

import groovyx.gpars.GParsPool
import groovyx.gpars.ParallelEnhancer
import java.util.concurrent.CopyOnWriteArrayList


@Typed(TypePolicy.STATIC)
class ReaderGPPPars {
	
	def readFile(String path) {
		List<List<String>> result = [][]
		new File(path).withReader{ reader ->
			reader.eachLine{ line ->
				result.add(line.split(",") as List)
			}
		}
		return result
	}

	def String processCell(String header, String cell) {
		if (processors.containsKey(header)) {
			def func = processors.get(header) find { true } value
			func(cell)
		} else {
			return cell
		}
	}

	@Typed(TypePolicy.MIXED)
	def processData(List<String> headers, List<List<String>> rows) {
		def newHeaders = headers collect { header ->
			if (processors.containsKey(header)) {
				processors.get(header) find { true } key
			} else {
				header
			}
		}
		
		ParallelEnhancer.enhanceInstance(rows)
		def newRows = new CopyOnWriteArrayList<List<String>> ()
		rows.eachParallel { row ->
			List<String> newRow = []
			row.eachWithIndex { String column, Integer colIndex ->
				String header = headers.get(colIndex)
				newRow.add(processCell(header, column))
			}
			newRows.add(newRow)	
		}
		
		return [newHeaders] + newRows
	}
	
	Closure complicated1 = { input ->
		(input as BigDecimal) * 30000000000l / 333333333l
	}
	
	Closure complicated2 = { input ->
		try {
			(input as BigDecimal) / 20000000l * Math.pow(input as Double, 20)
		} catch (e) {
			input
		}
	}
	
	Closure complicated3 = { input ->
		10 * (input as Double)
	}

	
	Map<String,Map<String, Closure>> processors = [
			"col1": ["Col1": complicated1],
			"col2": ["Column2": complicated2],
			"col3": ["Col3": complicated1],
			"col4": ["4": complicated3 << complicated1],
			"col5": ["five": complicated2],
			"col6": ["s-i-x": complicated2],
		]
	
	static main(args) {
		def rG = new ReaderGPPPars()
		
		List<List<String>> result = rG.readFile(Config.csvfile)
		def headers = result.get(0)
		def rows = result[1..(result.size() - 1)]		
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




class SequenceMatcher(stringA: String, stringB:String) {
	
	def ratio(): Double = {
		(2.0 * numOfMatches()) / (StringA.length() + stringB.length())
	}
	
	def numOfMatches(): Long = {
		stringA.intersect(StringB).length()
	}
}

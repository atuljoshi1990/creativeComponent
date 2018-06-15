val dataset1 = sc.textFile("free-zipcode-database.csv")
val dataset2 = sc.textFile("country-capitals.csv")
:paste SequenceMatcher.scala
val header1 = dataset1.first()
val header2 = dataset2.first()
val fDataset1 = dataset1.filter(row => row != header1)
val fDataset2 = dataset2.filter(row => row != header2)
val dat1 = fDataset1.map(r => r.split(","))
val dat2 = fDataset2.map(r => r.split(","))
val datArr1 = dat1.collect
val datArr2 = dat2.collect
var totalRowRatio:Double = 0;
var bigLength:Int = 0;
var smallLength:Int = 0;
if(datArr1.length > datArr2.length) {
	bigLength = datArr1.length;
	smallLength = datArr2.length;
} else { 
	bigLength = datArr2.length;
	smallLength = datArr1.length;
}
for(i <- 0 to (smallLength-1)) {
	var firstDatRow = datArr1(i);
	var secondDatRow = datArr2(i);
	var rowRatioSum:Double = 0;
	for(k <- 0 to (firstDatRow.length-1)){
		var valueF = firstDatRow(k);
		var valueRatio:Double = 0;
		for(l <- 0 to (secondDatRow.length-1)){
			var valueS = secondDatRow(l);
			var ratio = new SequenceMatcher(valueF, valueS).ratio();
			if(ratio > valueRatio){
				valueRatio = ratio;
			}
		};
		rowRatioSum = rowRatioSum + valueRatio;	
	};
	rowRatioSum = rowRatioSum/firstDatRow.length;
	totalRowRatio = totalRowRatio + rowRatioSum;
	println(rowRatioSum);
}
val degreeOfConstraintMatching = totalRowRatio/smallLength;

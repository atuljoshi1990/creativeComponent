val dataset1 = sc.textFile("country-capitals/part*")
val dataset2 = sc.textFile("free-zipcode-database/part*")
:paste SequenceMatcher.scala
val dat1 = dataset1.map(r => r.split(","))
val dat2 = dataset2.map(r => r.split(","))
val datArr1 = dat1.collect
val datArr2 = dat2.collect
var totalColumnRatioCount:Double = 0
var bigLength:Int = 0;
var smallLength:Int = 0;
if(datArr1.length > datArr2.length) {
	bigLength = datArr1.length;
	smallLength = datArr2.length;
} else { 
	bigLength = datArr2.length;
	smallLength = datArr1.length;
}
for(i <- 0 to (datArr1.length-1)) {
	var frstClmn = datArr1(i);
	var columnRatioCount:Double = 0; 
	for(j <- 0 to (datArr2.length-1)) { 
		var scndClmn = datArr2(j);
		var cRC:Double = 0; 
		for(k <- 0 to 2) { 
			var ratio = new SequenceMatcher(frstClmn(k),scndClmn(k)).ratio();
			cRC = cRC + ratio;
		}; 
		cRC = cRC/3; 
		if(cRC > columnRatioCount) {
			columnRatioCount = cRC;
		};
	};
	totalColumnRatioCount = totalColumnRatioCount + columnRatioCount;
}
val extraColumnCount = bigLength - smallLength;
totalColumnRatioCount = totalColumnRatioCount - extraColumnCount;
val degreeOfStructureMatching = totalColumnRatioCount/bigLength;

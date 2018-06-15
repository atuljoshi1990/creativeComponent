import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler};

val fileName = "country-capitals”;
var dataFrame = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(fileName+".csv”);
val originalDataFrame = dataFrame;
var tempAccuracy:Double = 0;
var modelList = List[org.apache.spark.ml.PipelineModel]();
for(i <- 0 to dataFrame.columns.length-1) {
	val header_indexer = new StringIndexer().setInputCol(dataFrame.columns(i)).setOutputCol(dataFrame.columns(i)+"_INDEXER").fit(dataFrame); 
	dataFrame = header_indexer.transform(dataFrame);
}
for(i <- 0 to (dataFrame.columns.length-1)/2) {
	dataFrame = dataFrame.drop(dataFrame.col(dataFrame.columns(0)));
}
var Array(trainingData, testData) = dataFrame.randomSplit(Array(0.7, 0.3));
for(i <- 0 to trainingData.columns.length-1){
	val clDataFrame = trainingData.select(trainingData(trainingData.columns(i)).as(trainingData.columns(i)));
	val map = Map(trainingData.columns(i) -> 0);
	val clmnDataFrame = clDataFrame.na.fill(map);
	var finalClmnDataFrame = clmnDataFrame.withColumn("label", lit(trainingData.columns(i).dropRight(8)));
	val label_indexer = new StringIndexer().setInputCol("label").setOutputCol("HEADER_INDEXER").fit(finalClmnDataFrame);
	val droppedClmn = finalClmnDataFrame.columns.slice(0,1);
	val features = new VectorAssembler().setInputCols(droppedClmn).setOutputCol("features");
	val layers = Array[Int](1,4,5,1);
	val trainer = new MultilayerPerceptronClassifier().setLabelCol("HEADER_INDEXER").setFeaturesCol("features").setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(20000000);
	val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(label_indexer.labels);
	val pipeline = new Pipeline().setStages(Array(label_indexer, features, trainer, labelConverter));
	val model = pipeline.fit(trainingData);
	modelList=model::modelList;
}
var headerMap = sc.emptyRDD[(String,String)];
var rdd = sc.emptyRDD[String];
for(i <- 0 to testData.columns.length-1){
	val clDataFrame = testData.select(testData(testData.columns(i)).as(testData.columns(i)));
	val map = Map(testData.columns(i) -> 0);
	val clmnDataFrame = clDataFrame.na.fill(map);
	var lmnDataFrame = clmnDataFrame.withColumn("label", lit(testData.columns(i))); 
	var columnAccuracy:Double=0;
	var predictions:org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = null;
	for(model <- modelList) {
		try {
			predictions = model.transform(lmnDataFrame);
			val evaluator = new MulticlassClassificationEvaluator().setLabelCol("HEADER_INDEXER").setPredictionCol("prediction").setMetricName("accuracy");
			val accuracy = evaluator.evaluate(predictions);
			if(accuracy > columnAccuracy) {
				columnAccuracy = accuracy;
			}
		} catch {
			case unknown:Exception => {
			}
		}
	};
	if(columnAccuracy > 0.7) {
		var dataType = "";
		val headerName = predictions.columns(0).dropRight(8);
		val df = originalDataFrame.select(headerName);
		df.dtypes.foreach{f => dataType = f._2.dropRight(4)}
		val sequence = Seq(headerName+":"+dataType);
		if(rdd.isEmpty) {
			rdd = sc.parallelize(sequence);
		} else {
			rdd = rdd.union(sc.parallelize(sequence));
		}
	}
	tempAccuracy = tempAccuracy + columnAccuracy;
}
headerMap = rdd.map(record => record.split(":")).map(a =>(a(0),a(1)));
val finalAccuracy = tempAccuracy/originalDataFrame.columns.length;
var uniqueMap = sc.emptyRDD[(String,String)];
var finalMap = sc.emptyRDD[(String,String)];
val totalRecords = originalDataFrame.count;
val clmns = originalDataFrame.columns;
var clmnsWithUniqueNess = new Array[String](clmns.length);
for (i <- 0 to (clmns.length-1)) {
	val clVal = originalDataFrame.select(clmns(i)).distinct().count();
	if(clVal == totalRecords) {
		clmnsWithUniqueNess(i) = clmns(i)+":Unique";
	} else {
		clmnsWithUniqueNess(i) = clmns(i)+":Nonunique”;
	}
}
val fRdd = sc.parallelize(clmnsWithUniqueNess);
val fData = fRdd.map(record => record.split(":"));
uniqueMap = fData.map(a => (a(0), a(1)));
val jt = uniqueMap.join(headerMap);
jt.coalesce(1).map(a => a._1 + "," + a._2._2.slice(0,a._2._2.length) + "," + a._2._1).saveAsTextFile(fileName);

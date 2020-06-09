import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;


public class test {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Java Spark SQL Example").setMaster("local[2]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //StructType schema = new StructType().add("movieid", "integer").add("title", "string").add("genre", "string");

        // Define schema of the movie CSV file
        StructType movie_schema = new StructType(new StructField[]{
                new StructField("movieid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("title", DataTypes.StringType, false, Metadata.empty()),
                new StructField("genre", DataTypes.StringType, false, Metadata.empty())
        });

        // Define schema of the reviews CSV file
        StructType reviews_schema = new StructType(new StructField[]{
                new StructField("userid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("movieid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("rating", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("timestamp", DataTypes.IntegerType, false, Metadata.empty())

        });


        String moviefile = "/Users/ashu/Documents/SCU/Spring 2020/Big Data/Assignment-3/Assignment3_datasets/DataSet4_large/movies_large.csv";
        String reviewsfile = "/Users/ashu/Documents/SCU/Spring 2020/Big Data/Assignment-3/Assignment3_datasets/DataSet4_large/reviews_large.csv";

        //Read the CSV file to a DataSet
        Dataset<Row> movies_df = spark.read().format("csv").option("header","true").schema(movie_schema).option("mode","PERMISSIVE").load(moviefile);
        Dataset<Row> reviews_df = spark.read().format("csv").option("header","true").schema(reviews_schema).option("mode","PERMISSIVE").load(reviewsfile);

        //Consider only the top 10 movies with most number of reviews
        Dataset<Row> updated_df = movies_df.select("movieid","title");
        Dataset<Row> rev_updated = reviews_df.groupBy("movieId").agg(functions.count("rating")).select(functions.col("movieId").alias("movieId"), functions.col("count(rating)").alias("num_ratings")).sort(functions.desc("num_ratings")).limit(10);
        rev_updated.show();
        updated_df.show();

        Dataset<Row> final_df =  updated_df.join(rev_updated, updated_df.col("movieid").equalTo(rev_updated.col("movieid")),"inner").select(functions.col("num_ratings"), functions.col("title"));

        //Write the DataSet to a csv file.
        final_df.coalesce(1).write().mode(SaveMode.Overwrite).csv("out");

    }

}
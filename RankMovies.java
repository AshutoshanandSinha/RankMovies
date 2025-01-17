import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.jetty.util.ArrayQueue;
import java.util.ArrayList;
import java.util.List;


public class RankMovies {

    //class variables
    private SparkConf conf;
    private SparkSession spark;

    public void setConf() {
        //setting the Spark Context and adding tweak parameters to it
        this.conf = new SparkConf().setAppName("Java Spark Rank Movies").setMaster("local[8]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    }

    public void setSpark() {
        this.spark = SparkSession.builder().config(this.conf).getOrCreate();
    }

    private String movies_csv_file;
    private String review_csv_file;
    private String outpath;

    //Construtor
    public RankMovies( String movies_csv_file, String review_csv_file, String outpath){
        this.movies_csv_file = movies_csv_file;
        this.review_csv_file = review_csv_file;
        this.outpath = outpath;
        this.setConf();
        this.setSpark();
    }

    // Method for applying Schemas
    private List<Dataset<Row>> apply_Schemas(){

        List<StructType> schema_list = define_Schemas();

        //Read the CSV file to a DataSet
        Dataset<Row> movies_ds = this.spark.read().format("csv").option("header","true").schema(schema_list.get(0)).option("mode","PERMISSIVE").load(this.movies_csv_file);
        Dataset<Row> reviews_ds = this.spark.read().format("csv").option("header","true").schema(schema_list.get(1)).option("mode","PERMISSIVE").load(this.review_csv_file);

        List<Dataset<Row>> datasets = new ArrayList<Dataset<Row>>();
        datasets.add(movies_ds);
        datasets.add(reviews_ds);
        return datasets;
    }
    // Method to define the Schemas for input file
    private  List<StructType> define_Schemas(){
        List<StructType> schema_list = new ArrayQueue<StructType>();

        // Define schema of the movie CSV file
        StructType movie_schema = new StructType(new StructField[]{
                new StructField("movieid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("title", DataTypes.StringType, false, Metadata.empty()),
                new StructField("genre", DataTypes.StringType, false, Metadata.empty())
        });
        schema_list.add(movie_schema);

        // Define schema of the reviews CSV file
        StructType reviews_schema = new StructType(new StructField[]{
                new StructField("userid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("movieid", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("rating", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("timestamp", DataTypes.IntegerType, false, Metadata.empty())

        });

        schema_list.add(reviews_schema);

        return schema_list;
    }

    //Spark job using Dataset to find the 10 movies with highest number of reviews.
    private void get_top_reviewed(){
        long startTime = System.currentTimeMillis();
        List<Dataset<Row>> datasets = this.apply_Schemas();
        Dataset<Row> movies_ds = datasets.get(0);
        Dataset<Row> review_ds = datasets.get(1);

        //Consider only the top 10 movies with most number of reviews
        Dataset<Row> movies_upd_ds = movies_ds.select("movieid","title");
        Dataset<Row> review_upd_ds = review_ds.groupBy("movieId").agg(functions.count("rating")).select(functions.col("movieId").alias("movieId"), functions.col("count(rating)").alias("num_ratings")).sort(functions.desc("num_ratings")).limit(10);


        Dataset<Row> final_ds =  review_upd_ds.join(movies_upd_ds, movies_upd_ds.col("movieid").equalTo(review_upd_ds.col("movieid")),"inner").select(functions.col("num_ratings"), functions.col("title"));

        //Write the DataSet to a csv file.
        final_ds.coalesce(1).write().mode(SaveMode.Overwrite).csv(this.outpath);

        long duration = (System.currentTimeMillis() - startTime)/1000;  //Total execution time in seconds
        System.out.println(duration+" seconds");
    }

    //Spark job using Dataset to find average reviews over 4 stars.
    private void get_avg_rating(){
        long startTime = System.currentTimeMillis();
        List<Dataset<Row>> datasets = this.apply_Schemas();
        Dataset<Row> movies_ds = datasets.get(0);
        Dataset<Row> review_ds = datasets.get(1);

        Dataset<Row> movies_upd_ds = movies_ds.select("movieid","title");
        Dataset<Row> review_ds_ratings = review_ds.groupBy("movieId").agg(functions.count("rating")).select(functions.col("movieId").alias("movie_Id"), functions.col("count(rating)").alias("num_ratings")).filter("num_ratings > 10");

        Dataset<Row> review_ds_avg = review_ds.groupBy("movieId").agg(functions.avg("rating")).select(functions.col("movieId").alias("movieId"), functions.col("avg(rating)").alias("avg_ratings")).filter("avg_ratings > 4");

        Dataset<Row> review_upd_final = review_ds_ratings.join(review_ds_avg, review_ds_ratings.col("movie_Id").equalTo(review_ds_avg.col("movieId")), "inner" ).select( functions.col("movieId"), functions.col("avg_ratings"));

        Dataset<Row> final_ds =  review_upd_final.join(movies_upd_ds, movies_upd_ds.col("movieid").equalTo(review_upd_final.col("movieid")),"inner").select(functions.col("avg_ratings"), functions.col("title"));

        //Write the DataSet to a csv file.
        final_ds.coalesce(1).write().mode(SaveMode.Overwrite).csv(this.outpath);

        long duration = (System.currentTimeMillis() - startTime)/1000;  //Total execution time in seconds
        System.out.println(duration+" seconds");
    }

    public static void main(String[] args) {

        String moviefile = "/Users/ashu/Documents/SCU/Spring 2020/Big Data/Assignment-3/Assignment3_datasets/DataSet4_large/movies_large.csv";
        String reviewsfile = "/Users/ashu/Documents/SCU/Spring 2020/Big Data/Assignment-3/Assignment3_datasets/DataSet4_large/reviews_large.csv";
        String outpath = "Users/ashu/Documents/SCU/Spring 2020/Big Data/Assignment-3/Assignment3_datasets/DataSet4_large/output";

        RankMovies rank_movies = new RankMovies(moviefile,reviewsfile,outpath);
        rank_movies.get_top_reviewed();
        //rank_movies.get_avg_rating();
    }

}
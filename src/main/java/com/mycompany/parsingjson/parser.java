/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.parsingjson;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.recommendation.ALS.Rating;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions.*;
/**
 *
 * @author Sayed
 */
public class parser {
    
    public static void main(String[] args) throws AnalysisException {
        
         
         SparkSession ss = SparkSession.builder()
                .appName("parser")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///D://")
                .getOrCreate();
                Dataset<Row> df =ss
                 .read()
                 .json("C:\\Users\\Sayed\\Documents\\NetBeansProjects\\BookRecommender01\\output1\\datatest.json").toDF();
            
                    df.show();
                    df.createOrReplaceTempView("v1");
                    df.printSchema();
                  
                 Dataset<Row> ndf =  df.select("recommendations.rating","recommendations.ISBN").alias("rating");
                 ndf.printSchema();
                   
                 ndf.show();
                 //Dataset<Row> Rating = ndf.select("recommendations.rating").alias("rating");
                 //Dataset<Row> d1= ndf.select("recommendations.rating","recommendations.ISBN").coalesce(2);
                 //d1.show();
                 System.out.println("loooooooooooooooooooooooooooooooooooooooool");
                  
                
}
}
# Pig S3Storer

A [UDF StoreFunc](http://pig.apache.org/docs/r0.8.0/udf.html#Store+Functions) for [Apache Pig](http://pig.apache.org/) designed to bulk-load data into [Amazon S3](http://aws.amazon.com/s3/). Inspired by [pig-redis](https://github.com/mattb/pig-redis) store function.

## Compiling and running

Compile:

Dependencies are automatically retrieved using [gradle](http://www.gradle.org/). Gradle itself is automatically retrieved using the gradle wrapper.

    $ ./gradlew clean jar

Use:

    $ pig
    grunt> REGISTER build/libs/pig-s3.jar;
    grunt> a = LOAD 'somefile.tsv' USING PigStorage() AS ( filename:chararray, content:chararray );
    grunt> STORE a INTO '/path/segment/' USING com.brunobonacci.pig.s3.S3Storer('s3://_accessKey_:_secretKey_@BUCKET/prefix/for/data/', 'plain/text');

The build process produces a jar with all necessary dependencies.

## Why would you need that?

This StoreFunc allows you to create a S3 entry for EVERY row in your datafile.
This comes useful for high volumes web-sites that have content that only changes once a day.
In that case you can generate the data using a UDF function in pig (maybe as JSON or XML)
and upload them as separate files into S3.

For example consider that your e-commerce catalog only updates once a day, you can then genereate a file
that contains two clomuns the SKU (product number) and a JSON string with the product information.
The file is going to look like This

Sample file contect:
```
SKU-123-abc-456.json  \t  { "product_name":"Item 1", "description":"Some text" ... }
SKU-456-xcc-978.json  \t  { "product_name":"Item 2", "description":"Some text" ... }
SKU-354-gfh-678.json  \t  { "product_name":"Item 3", "description":"Some text" ... }
```

by giving entering the following line into pig:
```
STORE file INTO '/update/2013-10-22' USING com.brunobonacci.pig.s3.S3Storer('s3://_accessKey_:_secretKey_@my-catalog/catalog/data/', 'application/json');
```
This will create the following directory structure into S3

```
my-catalog (bucket)
   \-- catalog
      \-- data
         \-- update
            \-- 2013-10-22
               \--| SKU-123-abc-456.json
                  | SKU-456-xcc-978.json
                  | SKU-354-gfh-678.json

```

Where the content of every json file will be the content of the second column in the file.

The S3Storer has three contructors:

  - `S3Storer( String s3_uri )`
  - `S3Storer( String s3_uri, String contentType)`
  - `S3Storer( String accessKey, String secretKey, String bucketName, String path, String contentType )`

Of course you can leverage (Pig's parameter substitution)[http://wiki.apache.org/pig/ParameterSubstitution] to parametrize all those info. For example:

```
pig -p $LOCATION=s3://_accessKey_:_secretKey_@my-catalog/catalog/data/' -p DATE=$(date +"%Y-%m-%d") MyLoadScript.pig
```
and in the script put:
```
...
STORE file INTO '$DATE' USING com.brunobonacci.pig.s3.S3Storer('$LOCATION', 'application/json');
```

## License

Distributed under [Apache Lincense 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

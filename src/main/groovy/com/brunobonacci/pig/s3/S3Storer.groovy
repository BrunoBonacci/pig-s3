package com.brunobonacci.pig.s3

/*
  Copyright (c) 2013 Bruno Bonacci. All Rights Reserved.

  This file is part of the pig-riak project.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Third-party Licenses:

  All third-party dependencies are listed in build.gradle.
*/

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.fs.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.*;
import org.apache.pig.data.Tuple;

import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.model.*;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.Constants;
import org.jets3t.service.multi.SimpleThreadedStorageService;

import java.io.*;
import java.util.*;
import java.net.URI;

/**
 * This store function pushes the rows into Amazon S3.
 * The first element of a row is the key, while the second is the content.
 *
 * This implementation uses JetS3t library to connect to S3.
 * Every time putNext() is called a S3Object is created and put into a _batch
 * once the batch size reaches a certain size (BATCHING_FACTOR * _SERVICE_THREADS)
 * it pushes the objects to the specified S3 bucket using a group of parallel threads.
 *
 * NOTE: if you increase too much the _SERVICE_THREADS it's likely to get a lot
 * of rejection from S3.
 */
public class S3Storer extends StoreFunc {

    // BATCH SIZE => batching_factor * service_threads
    protected int _batching_factor = 50;
    // number of threads to use for upload
    // note that if this value is too high
    // you will receive HTTP 503 Slow down
    // from Amazon S3
    private static final int DEFAULT_SERVICE_THREADS = 5;
    protected int _service_threads = DEFAULT_SERVICE_THREADS;
    protected int _admin_threads = 50;

    protected String _accessKey;
    protected String _secretKey;
    protected String _bucketName;
    protected String _path;
    protected String _contentType;


    protected RecordWriter _writer;
    protected def          _s3;
    protected def          _s3Multi;
    protected def          _bucket;
    protected def          _location;
    protected def          _properties;
    protected def          _batch = [];

    public S3Storer(String uri) {
        this(uri, "text/plain");
    }


    public S3Storer(String uri, String contentType) {
        this(uri, contentType, "$DEFAULT_SERVICE_THREADS");
    }

    public S3Storer(String uri, String contentType, String numUploadThreads) {
        URI u = new URI(uri).parseServerAuthority();
        if( u.scheme != 's3' && u.scheme != 's3n' )
            throw new IllegalArgumentException("Unsopported AWS S3 scheme in uri: $uri");

        _init( u.userInfo?.split(':')?.getAt(0),
               u.userInfo?.split(':')?.getAt(1),
               u.host,
               u.path,
               contentType,
               numUploadThreads);
    }


    public S3Storer(String accessKey, String secretKey,
                    String bucketName, String path, String contentType) {
        _init(accessKey, secretKey, bucketName, path, contentType, "$DEFAULT_SERVICE_THREADS" );
    }

    public S3Storer(String accessKey, String secretKey,
                    String bucketName, String path, String contentType,
                    String numUploadThreads) {
        _init(accessKey, secretKey, bucketName, path, contentType, numUploadThreads );
    }


    protected void _init(String accessKey, String secretKey,
                         String bucketName, String path, String contentType,
                         String numUploadThreads) {
        _accessKey = accessKey;
        _secretKey = secretKey;
        _bucketName = bucketName;
        _path      = path;
        _contentType = contentType;

        checkValue( "accessKey",  _accessKey );
        checkValue( "secretKey",  _secretKey );
        checkValue( "bucketName", _bucketName );
        checkValue( "path", _path );
        checkValue( "contentType", _contentType );
        checkIntValue( "numUploadThreads", numUploadThreads, 1);

        _service_threads = Integer.parseInt(numUploadThreads);

        _path = _path.replaceAll( /\/+$/, '' );

        // Load your default settings from jets3t.properties file on the classpath
        _properties = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);

        // Override default properties (increase number of connections and
        // threads for threaded service)
        _properties.setProperty("httpclient.max-connections", "${_service_threads + _admin_threads}");
        _properties.setProperty("threaded-service.max-thread-count", "$_service_threads");
        _properties.setProperty("threaded-service.admin-max-thread-count", "$_admin_threads");
        _properties.setProperty("httpclient.retry-max", "10" );
    }

    private void checkValue( String field, String value ){
        if( value == null || value.trim() == '' )
            throw new IllegalArgumentException( "Invalid or missing value for field $field '$value'");
    }

    private void checkIntValue( String field, String value, int min ){
        checkValue( field, value );
        try {
            if( Integer.parseInt( value ) < min)
                throw new IllegalArgumentException( "Invalid value for field $field '$value', the mininum value is: $min");
        } catch( NumberFormatException x){
            throw new IllegalArgumentException( "Invalid value for field $field '$value'");
        }
    }

    @Override
    public OutputFormat getOutputFormat() {
        return new NullOutputFormat(){

            @Override
            public RecordWriter getRecordWriter(TaskAttemptContext ctx) {
                return new RecordWriter(){
                    public void write(Object key, Object value) { }
                    /** This is VERY IMPORTANT to make sure that last batch is pushed as well */
                    public void close(TaskAttemptContext ctxx) { putBatch() }
                };
            }
          };
    }

    @Override
    public void putNext(Tuple f) throws IOException {
        if(f.get(0) == null) {
            return;
        }

        String key = f.get(0).toString();
        Object value = f.getAll()?.get(1);
        if(value != null) {
            String content = value.toString();
            // upload object (_location contains trailing /)
            def s3obj = new S3Object( _bucket, "${_location}$key", content);
            s3obj.contentType = _contentType;
            batchAndPutObject( s3obj );
        }
    }


    /** this method batches the input up and when the batch is full submit the put request in parallel threads */
    protected synchronized void batchAndPutObject( S3Object obj ) throws IOException {
        _batch += obj;
        if( _batch.size() >= _batching_factor * _service_threads ) {
            putBatch();
        }
    }


    protected synchronized void putBatch() {
        if( _batch.size() > 0 ){
            _s3Multi.putObjects( _bucketName, _batch as S3Object[] );
            _batch = [];
        }
    }


    @Override
    public void prepareToWrite(RecordWriter writer) {
        _writer = writer;
        def login = new AWSCredentials( _accessKey, _secretKey );
        _s3 = new RestS3Service( login, "pig-s3 store", null, _properties );
        _s3Multi = new SimpleThreadedStorageService(_s3);
        _bucket = new S3Bucket( _bucketName );
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if( !location )
            throw new IllegalArgumentException("Invalid bucket name $location");

        _location = "$_path/$location";
        _location = _location.replaceAll( /\/+/, '/' );  // remove double //
        _location = _location.replaceAll( /\/+$/, '' );  // remove trailing /
        _location = _location.replaceAll( /^\/+/, '');   // remove leading /
        _location = _location.trim() != '' ?  "$_location/" : '';  // if location is present append /

    }



    @Override
    public String relToAbsPathForStoreLocation(String location, org.apache.hadoop.fs.Path curDir) throws IOException {
        return location;
    }
}

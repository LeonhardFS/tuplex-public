//
// Created by leonhards on 1/15/25.
//

#ifndef TUPLEX_S3TYPES_H
#define TUPLEX_S3TYPES_H

#ifdef BUILD_WITH_AWS

// Define which clien to use (per default).
#define USE_AWS_S3_CRT_CLIENT

#include <aws/s3/S3Client.h>
// models
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#ifdef USE_AWS_S3_CRT_CLIENT
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/DeleteObjectsRequest.h>
#include <aws/s3-crt/model/CopyObjectRequest.h>
#include <aws/s3-crt/model/CreateBucketRequest.h>
#endif

// Alias types to flip between Crt and regular S3 client.
namespace tuplex {

#ifdef USE_AWS_S3_CRT_CLIENT
    // Define which S3 client to use.
    using AwsS3Client=Aws::S3Crt::S3CrtClient;
    using AwsS3GetObjectRequest=Aws::S3Crt::Model::GetObjectRequest;
    using AwsS3PutObjectRequest=Aws::S3Crt::Model::PutObjectRequest;
    using AwsS3MultiPartUploadRequest=Aws::S3Crt::Model::CreateMultipartUploadRequest;
    using AwsS3CreateMultipartUploadRequest=Aws::S3Crt::Model::CreateMultipartUploadRequest;
    using AwsS3CompleteMultipartUploadRequest=Aws::S3Crt::Model::CompleteMultipartUploadRequest;
    using AwsS3UploadPartRequest=Aws::S3Crt::Model::UploadPartRequest;
    using AwsS3CompletedMultipartUpload=Aws::S3Crt::Model::CompletedMultipartUpload;
    using AwsS3CompletedPart=Aws::S3Crt::Model::CompletedPart;
    using AwsS3RequestPayer=Aws::S3Crt::Model::RequestPayer;
    static const auto AwsS3RequestPayerRequester=Aws::S3Crt::Model::RequestPayer::requester;
    static const auto AwsS3RequestPayerNotSet=Aws::S3Crt::Model::RequestPayer::NOT_SET;

    using AwsS3HeadObjectRequest=Aws::S3Crt::Model::HeadObjectRequest;

    using AwsS3ListObjectsV2Request=Aws::S3Crt::Model::ListObjectsV2Request;

    using AwsS3DeleteObjectsRequest=Aws::S3Crt::Model::DeleteObjectsRequest;
    using AwsS3Delete=Aws::S3Crt::Model::Delete;
    using AwsS3ObjectIdentifier=Aws::S3Crt::Model::ObjectIdentifier;

    using AwsS3CopyObjectRequest=Aws::S3Crt::Model::CopyObjectRequest;

    using AwsS3CreateBucketRequest=Aws::S3Crt::Model::CreateBucketRequest;
    // TODO: think for CrtClient of adding to config sensible defaults for target rates/part sizes.
    // https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/cpp/example_code/s3-crt/s3-crt-demo.cpp#L235

#else
    using AwsS3Client=Aws::S3::S3Client;
    using AwsS3GetObjectRequest=Aws::S3::Model::GetObjectRequest;
    using AwsS3PutObjectRequest=Aws::S3::Model::PutObjectRequest;
    using AwsS3MultiPartUploadRequest=Aws::S3::Model::CreateMultipartUploadRequest;
    using AwsS3CreateMultipartUploadRequest=Aws::S3::Model::CreateMultipartUploadRequest;
    using AwsS3CompleteMultipartUploadRequest=Aws::S3::Model::CompleteMultipartUploadRequest;
    using AwsS3UploadPartRequest=Aws::S3::Model::UploadPartRequest;
    using AwsS3CompletedMultipartUpload=Aws::S3::Model::CompletedMultipartUpload;
    using AwsS3CompletedPart=Aws::S3::Model::CompletedPart;
    using AwsS3RequestPayer=Aws::S3::Model::RequestPayer;
    static const auto AwsS3RequestPayerRequester=Aws::S3::Model::RequestPayer::requester;
    static const auto AwsS3RequestPayerNotSet=Aws::S3::Model::RequestPayer::NOT_SET;

    using AwsS3HeadObjectRequest=Aws::S3::Model::HeadObjectRequest;
    using AwsS3ListObjectsV2Request=Aws::S3::Model::ListObjectsV2Request;

    using AwsS3DeleteObjectsRequest=Aws::S3::Model::DeleteObjectsRequest;
    using AwsS3Delete=Aws::S3::Model::Delete;
    using AwsS3ObjectIdentifier=Aws::S3::Model::ObjectIdentifier;
    using AwsS3CopyObjectRequest=Aws::S3::Model::CopyObjectRequest;
    using AwsS3CreateBucketRequest=Aws::S3::Model::CreateBucketRequest;
#endif

    inline std::string AwsS3RequestPayerToString(const AwsS3RequestPayer& requester) {
        if(requester == AwsS3RequestPayerRequester)
            return "requester (" + std::to_string(static_cast<int>(requester)) + ")";
        if(requester == AwsS3RequestPayerNotSet)
            return "NOT_SET (" + std::to_string(static_cast<int>(requester)) + ")";
        return "UNKNOWN VALUE (" + std::to_string(static_cast<int>(requester)) + ")";
    }
}

#endif

#endif //TUPLEX_S3TYPES_H

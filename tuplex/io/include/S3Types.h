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

#ifdef USE_AWS_S3_CRT_CLIENT
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>
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
#endif
}

#endif

#endif //TUPLEX_S3TYPES_H

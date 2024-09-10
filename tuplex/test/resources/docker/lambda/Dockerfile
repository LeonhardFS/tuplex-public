# Dockerfile to create a local test container for the AWS Tuplex Lambda executor.
FROM public.ecr.aws/lambda/provided:al2

# Base image contains predefined variables for where code needs to get copied
# default values are:
# LAMBDA_TASK_ROOT=/var/task
# LAMBDA_RUNTIME_DIR=/var/runtime

RUN yum install -y unzip

ADD tplxlam.zip ${LAMBDA_TASK_ROOT}

RUN unzip -o ${LAMBDA_TASK_ROOT}/tplxlam.zip -d ${LAMBDA_TASK_ROOT} && rm ${LAMBDA_TASK_ROOT}/tplxlam.zip

RUN mv ${LAMBDA_TASK_ROOT}/bootstrap ${LAMBDA_RUNTIME_DIR}


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "function.handler" ]

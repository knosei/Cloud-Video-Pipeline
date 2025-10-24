# FROM ubuntu:22.04
FROM public.ecr.aws/lts/ubuntu:22.04


# Avoid interaction prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install ffmpeg, Python3, pip, and boto3
RUN apt-get update && \
    apt-get install -y ffmpeg python3-pip && \
    pip3 install boto3 && \
    apt-get clean

# Set working directory
WORKDIR /app

# Copy your FFmpeg processor script into the image
COPY processor.py .
COPY metrics.py /app/   
# Set default command to run the processor
CMD ["python3", "processor.py"]

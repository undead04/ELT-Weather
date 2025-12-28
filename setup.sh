#!/bin/bash

# Define the target directory for JARs
JAR_DIR="spark/jars"

# Create the directory if it doesn't exist
if [ ! -d "$JAR_DIR" ]; then
    echo "Creating directory: $JAR_DIR"
    mkdir -p "$JAR_DIR"
fi

# List of JARs to download
declare -A JARS=(
    ["hadoop-aws-3.3.4.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ["aws-java-sdk-bundle-1.12.320.jar"]="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.320/aws-java-sdk-bundle-1.12.320.jar"
    ["delta-spark_2.12-3.2.0.jar"]="https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar"
    ["delta-storage-3.2.0.jar"]="https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"
    ["postgresql-42.7.3.jar"]="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
    ["stax2-api-4.2.1.jar"]="https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar"
    ["woodstox-core-6.2.4.jar"]="https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.4/woodstox-core-6.2.4.jar"
)

echo "Downloading JARs..."

for JAR_NAME in "${!JARS[@]}"; do
    URL="${JARS[$JAR_NAME]}"
    FILE_PATH="$JAR_DIR/$JAR_NAME"

    if [ -f "$FILE_PATH" ]; then
        echo " - $JAR_NAME already exists, skipping."
    else
        echo " - Downloading $JAR_NAME..."
        curl -L -o "$FILE_PATH" "$URL"
        if [ $? -eq 0 ]; then
             echo "   Successfully downloaded $JAR_NAME"
        else
             echo "   Failed to download $JAR_NAME from $URL"
             exit 1
        fi
    fi
done

echo "Setup complete! All JARs are in $JAR_DIR"

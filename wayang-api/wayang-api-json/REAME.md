# Wayang JSON REST API

## Getting Started

### 1. Package the Project

```bash
./mvnw clean package -pl :wayang-assembly -Pdistribution
```

### 2. Starting the REST API as a Background Process

```bash
cd wayang-assembly/target/
tar -xvf apache-wayang-assembly-1.1.1-dist.tar.gz
cd wayang-1.1.1
./bin/wayang-submit org.apache.wayang.api.json.Main
```

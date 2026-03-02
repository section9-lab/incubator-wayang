# Running Apache Wayang on Windows

This guide helps Windows users successfully build and run Apache Wayang.

Many users encounter issues related to Hadoop, winutils, and environment variables.  
Follow these steps carefully.

---

## 1. Install Java 17

Download and install:

https://adoptium.net/

After installation verify:
java -version

---

## 2. Install Maven

Download:

https://maven.apache.org/download.cgi

Extract and add Maven `/bin` to your **System PATH**.

Verify:
mvn -version

---

## 3. Install Hadoop winutils

Wayang requires Hadoop utilities on Windows.

### Download winutils

Download from:

https://github.com/steveloughran/winutils

Choose a version matching Hadoop 3.x.

### Setup

Create directory: C:\hadoop\bin

Place: winutils.exe

inside `bin`.

---

## 4. Set Environment Variables

Open:

**System Properties → Environment Variables**

### Add:

#### HADOOP_HOME
C:\hadoop

#### Add to PATH
C:\hadoop\bin

Restart terminal after saving.

---

## 5. Verify Hadoop setup

Run:
winutils.exe ls


If no error appears → setup is correct.

---

## 6. Build Wayang

From project root:
./mvnw clean install -DskipTests

---

## 7. Common Issues

### ❌ winutils.exe not found
Ensure:

• file exists in `C:\hadoop\bin`  
• PATH includes the bin folder  

### ❌ HADOOP_HOME not set
Verify environment variable.

### ❌ Access denied errors
Run terminal as Administrator.

---

## 8. Notes

• Windows support requires winutils.  
• WSL (Windows Subsystem for Linux) can be used as an alternative.

---

You are now ready to run Apache Wayang on Windows.


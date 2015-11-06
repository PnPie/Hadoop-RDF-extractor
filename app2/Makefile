HADOOP = hadoop
# If you didn't set environment path for hadoop, need to set the path of it for HADOOP, by exemple the following
# HADOOP = /usr/local/hadoop/bin/hadoop
SRC = src/EXE2.java
BIN = bin
CLASS_DIR = $(BIN)/classes
TARGET_JAR = $(BIN)/EXE2.jar
INPUT_DIR = input
INTERMEDIATE_DIR = intermediate
OUTPUT_DIR = output
CLASS_IN_PACKAGE = org.myrdf.EXE2

all: $(TARGET_JAR)
	$(HADOOP) jar $(TARGET_JAR) $(CLASS_IN_PACKAGE) $(INPUT_DIR) $(INTERMEDIATE_DIR) $(OUTPUT_DIR)

#If there exists already $(TARGET_JAR), we will not execute the following
$(TARGET_JAR): $(SRC)
	mkdir -p $(CLASS_DIR)
	javac -classpath "`hadoop classpath`" -d $(CLASS_DIR) $(SRC) -Xlint
	jar -cvf $(TARGET_JAR) -C $(CLASS_DIR) ./

clean:
	rm -rf bin $(OUTPUT_DIR) $(INTERMEDIATE_DIR) $(BIN)

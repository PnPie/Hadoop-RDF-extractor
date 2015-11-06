HADOOP = hadoop
# If you didn't set environment path for hadoop, need to set the path of it for HADOOP, by exemple the following
# HADOOP = /usr/local/hadoop/bin/hadoop
SRC = src/EXE1.java
BIN = bin
CLASS_DIR = $(BIN)/classes
TARGET_JAR = $(BIN)/EXE1.jar
INPUT_DIR = input
OUTPUT_DIR = output
CLASS_IN_PACKAGE = org.myrdf.EXE1

all: $(TARGET_JAR)
	$(HADOOP) jar $(TARGET_JAR) $(CLASS_IN_PACKAGE) $(INPUT_DIR) $(OUTPUT_DIR)

$(TARGET_JAR): $(SRC)
	mkdir -p $(CLASS_DIR)
	javac -classpath "`hadoop classpath`" -d $(CLASS_DIR) $(SRC) -Xlint
	jar -cvf $(TARGET_JAR) -C $(CLASS_DIR) ./

clean:
	rm -rf $(OUTPUT_DIR) $(BIN)

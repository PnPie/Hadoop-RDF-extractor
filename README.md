# RDF-Statistics-information-extractor
4 Hadoop Map/Reduce applications to collect and extract statistical information from RDF data given.

The given data is RDF(Resource Description Framework) in standard N-Triples format(subject-predicate-object expressions), a W3C proposed standard for conceptual description of information that is implemented in web resources.

## N-Triple Count

Calculating the appearance of every URI and literal element in each position(subject, predicate, object)

## App2

Collecting the 10 most frequently-appearing predicates in data set and sorting them.

## App3

Collecting the 10 most freauently-appearing classes in data set and sorting them

## App4

Collecting the first 10 subjects with the largest number of distinct predicates

## Execution
```
hadoop jar $(TARGET_JAR) $(CLASS) $(INPUT_DIR) $(OUTPUT_DIR)
```

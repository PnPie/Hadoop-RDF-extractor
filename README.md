# RDF-Statistics-information-extractor
Hadoop Map/Reduce applications to collect and extract statistical information from RDF data given.

The given data is RDF(Resource Description Framework) in standard N-Triples format(subject-predicate-object expressions), a W3C proposed standard for conceptual description of information that is implemented in web resources.

## N-Triple Count

Calculating the appearance of every URI and literal element in each position(subject, predicate, object)

**Execution:**
```
bin/triplecount $(PATH_TO_HADOOP) $(INPUT_FOLDER)
```

## Top K Predicates

Collecting the 10 most frequently-appearing predicates in data set and sorting them.

**Execution:**
```
bin/topkpredicate $(PATH_TO_HADOOP) $(INPUT_FOLDER)
```

## Subjects with most predicates

Collecting the first 10 subjects with the largest number of distinct predicates

**Execution:**
```
bin/subpredicates $(PATH_TO_HADOOP) $(INPUT_FOLDER)
```

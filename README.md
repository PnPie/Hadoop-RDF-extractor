# RDF-Statistics-information-extractor
- 4 Hadoop Map/Reduce applications to collect and calculate the statistical informations from RDF data given.

The given data is RDF(Resource Description Framework) in standard N-Triples format(subject-predicate-object expressions), a W3C proposed standard for conceptual description of information that is implemented in web resources.

## app1

Calculating the appearance of every URI and literal element in each position(subject, predicate, object)

## app2

Collecting the 10 most frequently-appearing predicates in data set and sorting them.

## app3

Collecting the 10 most freauently-appearing classes in data set and sorting them

## app4

Collecting the first 10 subjects with the largest number of distinct predicates

## To execute

The Makefile is provided for each application to compile the source codes and execute the program with the given data in "input" folder, the results will be saved in the "output" folder.

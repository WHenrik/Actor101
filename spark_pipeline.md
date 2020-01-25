Pipeline

basically following the paper

- read all the files
- label each entry with the column name using the schema
- flatten everything and do the preaggregation
- do groupBy to get the global partitioning
- extract the attibuteSet (drop the cell content to only keep the column info)
- derive the inclusionList
- aggregate the inclusionList to filter complete inclusions (true INDs)

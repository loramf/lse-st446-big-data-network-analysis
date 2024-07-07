In this report “Analysis of networks” the author presented an analysis of Twitter data, which consists of data collection and pre-processing, analysis of graph data representations, and topic modelling of collected Twitter posts. 

The abstract well describes what is the problem studied in the report, why the problem is a problem, and the solution of the problem. The introduction / abstract could have summarised the main findings of the study. 

The solution concepts include using streaming computation for data collection, graph data representation for manipulating graph data representations, using graph node embeddings and data visualisations. The graph node embedding is based on using node2vec, which is implemented by combining information gathered by random walks of fixed length starting at nodes of the input graph, with word2vec. The report could have provided more explanations of these key solution concepts, and cited references. 

The implementation is based on using PySpark for data collection and data processing. The data collection is based on using Spark Streaming to collect Twitter data. The graph analysis is based on using graphframes. The topic modelling is based on using the LDA implementation available in Spark’s ML library. The code is well structured and commented throughout. 

The dataset consists of 1.6M tweets collected by the author over a period of 24 hours. Some of the basic properties of the dataset are analysed in one of the notebooks, e.g. distribution of languages and tweets versus retweets. The dataset is of small or moderate size. 

The numerical evaluation consists of evaluating various graph queries using graphframes API and reporting results by presenting numerical values or data visualisations. A graph node embedding is used (node2vec) to represent graph vertices by numerical vectors, which are visualised by using by projecting onto a two-dimensional space (using t-SNE). Node2vec is implemented by generating random walks of fixed length starting at graph vertices, and feeding this into the word2vec.

Conclusion well summarised various components, including data collection, data analysis, and data visualisation. The conclusion could have summarised some of the key findings pertinent to distributed computation, e.g. any insights on the scalability of the solution, and efficiency gains of distributed computation. 

The presentation quality of the notebooks is good. The report is clear and well structured. The report is missing references. 

**78**

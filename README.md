# PageRank_Implementations
We have implemented PageRank algorithm in serial, parallel shared-memory and distributed (map-reduce) way, and we have made comparisons between the different implementation paradigms. This was our term project for CS425: Algorithms for Web-Scale Data, a CS technical elective course offered at Bilkent University, Fall 2018.
### Brief Info
We used OpenMP application programming interface that supports multi-platform shared memory multiprocessing programming in C++, and Apache Spark unified analytics engine for the distributed implementation (PySpark Map-Reduce). After implementing the algorithms, we tested them with three different datasets taken from Stanford Large Network Dataset collection, namely the NotreDame web graph with 1.5 Million egdes, the Google web graph with 5.1 Million edges, and the Patent Citation Network with 16.5 Million edges. Then we compared the amount of time taken to complete the execution in each of the implementations as well as the PageRank results.
### Findings
As expected, we observed that the parallel implementation takes shorter amount of time than the serial one. Also, we saw that the distributed implementation using Map-Reduce is actually really efficient compared to Parallel implementation. Also, data preprecessing is done faster in Spark. However, there is a trade-off of this result: Spark setup requires its own time. Also, the distributed implementation is not feasible with the calculation of PageRank leakage in case the graph has dead-ends. However, considering much larger graphs than the datasets we used, the Spark setup time will eventually become unsignificant, so it is better to use that implementation and not parallel one. Also, with more sophisticated methods of calculating the PageRank leakage due to dead ends, this problem can also be overcome in the distributed implementation.
### Further Details
Please check our [report](https://github.com/NaisilaPuka/PageRank_Implementations/blob/master/PR_Implementations_Report.pdf).
### Credits
1. [Naisila Puka](https://github.com/NaisilaPuka): Distributed (Map-Reduce) Implementation, Report
2. [Endi Merkuri](https://github.com/endimerkuri): Serial and Parallel Implementation, Convertion of datasets
3. [Fatbardh Feta](https://github.com/fatbardhfeta): PySpark functions and OpenMP API research

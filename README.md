# large-scale-lbfgs

## reference
Chen W, Wang Z, Zhou J. Large-scale L-BFGS using MapReduce[C]//Advances in Neural Information Processing Systems. 2014: 1332-1340.

Andrew G, Gao J. Scalable training of L 1-regularized log-linear models[C]//Proceedings of the 24th international conference on Machine learning. ACM, 2007: 33-40.

https://github.com/chokkan/liblbfgs.git

https://github.com/linkedin/ml-ease.git

## support
1. logistic regression using L1-regularization and L2-regularization
2. train/test a model in local-environment(single machine) or hadoop-environment
3. use avro file to save disk space and accelerate data loading
4. use a single configuration file to config runtime environment

## usage
run a local job:
java -jar <xxx.jar> <config-file>
run a hadoop job:
hadoop jar <xxx.jar> <config-file>

## todo
1. support more loss functions
2. run in spark environment

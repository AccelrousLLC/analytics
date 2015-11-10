# NR8 Analytics

This is a repository for backend analytics and workers.

## Directory Structure

`lambda/` - AWS Lambda functions.  Each folder inside of the `lambda` directory is its own separate deployable lambda package.

> Arnaud - decided it would probably be better to allow each lambda to have it's own dependency tree.

`emr` - AWS Elastic MapReduce tasks.
